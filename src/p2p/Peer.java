package p2p;

import comum.Limite;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;

/*
 * O arquivo é dividido em blocos cada peer pede blocos a vizinhos aleatórios e
 * também serve os blocos que já tem
 */
public class Peer {
    public static final int BLOCO = 256 * 1024;     // blocos maiores reduzem a troca de mensagens
    private static final int TRABALHADORES = 4;     // downloads em paralelo por peer
    private static final int PEDACO_ENVIO = 32 * 1024; // limite no upload

    private final int porta;
    private final long tamanho;
    private final int nBlocos;
    private final Limite upload;                    // um por peer
    private final BitSet tenho = new BitSet();      // mapa dos blocos que este peer possui
    private final Path temp;                        // null no seed, que lê direto do arquivo original
    private final FileChannel canal;
    private ServerSocket servidor;
    private final List<InetSocketAddress> vizinhos;
    private volatile boolean ativo = true;
    private final CountDownLatch concluido = new CountDownLatch(1);
    private volatile long inicio, fim;

    private Peer(int porta, long tamanho, Limite upload,
                 List<InetSocketAddress> vizinhos, Path origem) throws IOException {

        this.porta = porta;
        this.tamanho = tamanho;
        this.nBlocos = (int) ((tamanho + BLOCO - 1) / BLOCO);   // divisão com arredondamento para cima
        this.upload = upload;
        this.vizinhos = vizinhos;

        if (origem != null) {
            this.temp = null;
            this.canal = FileChannel.open(origem, StandardOpenOption.READ);
            this.tenho.set(0, nBlocos);
        }
        else {
            this.temp = Files.createTempFile("peer" + porta + "-", ".tmp");
            this.canal = FileChannel.open(temp, StandardOpenOption.READ, StandardOpenOption.WRITE);
        }
    }

    public static Peer seed(int porta, Path arquivo, Limite up) throws IOException {
        return new Peer(porta, Files.size(arquivo), up, new ArrayList<>(), arquivo);

    }

    public static Peer leecher(int porta, long tamanho, Limite up,
                               List<InetSocketAddress> vizinhos) throws IOException {

        return new Peer(porta, tamanho, up, vizinhos, null);
    }

    private int tamanhoBloco(int i) {
        return (int) Math.min(BLOCO, tamanho - (long) i * BLOCO);
    }
    // entrega blocos aos outros peers
    public void iniciarServidor() throws IOException {
        servidor = new ServerSocket();
        servidor.setReuseAddress(true);
        servidor.bind(new InetSocketAddress(porta), 200);

        Thread t = new Thread(() -> {
            while (ativo) {
                try {
                    Socket s = servidor.accept();
                    Thread h = new Thread(() -> atenderPedido(s));
                    h.setDaemon(true);
                    h.start();
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                    break;
                }
            }
        });



        t.setDaemon(true);
        t.start();
    }



    private void atenderPedido(Socket s) {

        try (s) {
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());

            int len = in.readInt();
            byte[] bytes = new byte[len];
            in.readFully(bytes);
            BitSet dele = BitSet.valueOf(bytes);

            BitSet candidatos;
            synchronized (tenho) {
                candidatos = (BitSet) tenho.clone();
            }

            candidatos.andNot(dele);

            if (candidatos.isEmpty()) {
                out.writeInt(-1);
                out.flush();
                return;
            }

            int idx = candidatos.nextSetBit(ThreadLocalRandom.current().nextInt(nBlocos));
            if (idx < 0) idx = candidatos.nextSetBit(0);

            int tam = tamanhoBloco(idx);
            ByteBuffer bb = ByteBuffer.allocate(tam);
            long pos = (long) idx * BLOCO;

            while (bb.hasRemaining()) {
                if (canal.read(bb, pos + bb.position()) < 0) {
                    break;
                }
            }

            out.writeInt(idx);
            byte[] dados = bb.array();

            for (int off = 0; off < tam; off += PEDACO_ENVIO) {
                int n = Math.min(PEDACO_ENVIO, tam - off);
                upload.consumir(n);
                out.write(dados, off, n);
            }

            out.flush();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        }
    }



    public void iniciarDownload() {
        inicio = System.nanoTime();

        for (int i = 0; i < TRABALHADORES; i++) {
            Thread t = new Thread(this::trabalhador);
            t.setDaemon(true);
            t.start();
        }
    }

    private void trabalhador() {

        Random rnd = ThreadLocalRandom.current();
        while (ativo && concluido.getCount() > 0) {

            InetSocketAddress v = vizinhos.get(rnd.nextInt(vizinhos.size()));
            byte[] meus;
            synchronized (tenho) {
                meus = tenho.toByteArray();
            }

            try (Socket s = new Socket()) {

                s.connect(v, 5000);
                DataOutputStream out = new DataOutputStream(s.getOutputStream());
                DataInputStream in = new DataInputStream(s.getInputStream());
                out.writeInt(meus.length);
                out.write(meus);
                out.flush();

                int idx = in.readInt();
                if (idx < 0) {
                    dormir(5); continue;
                }

                byte[] dados = new byte[tamanhoBloco(idx)];
                in.readFully(dados);
                receber(idx, dados);

            } catch (IOException e) {
                dormir(10);
                System.out.println(e.getMessage());
            }
        }
    }

    private void receber(int idx, byte[] dados) throws IOException {

        // duas threads podem receber o mesmo bloco ao mesmo tempo,  a segunda é descartada.
        synchronized (tenho) { if (tenho.get(idx)) return; }
        canal.write(ByteBuffer.wrap(dados), (long) idx * BLOCO);

        synchronized (tenho) {
            tenho.set(idx);

            if (tenho.cardinality() == nBlocos && concluido.getCount() > 0) {
                fim = System.nanoTime();
                concluido.countDown();
            }
        }
    }

    private static void dormir(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            System.out.println(ignored.getMessage());
        }
    }

    // espera o fim do download e devolve o tempo em segundos
    public double aguardar() throws InterruptedException {
        concluido.await();

        return (fim - inicio) / 1e9;
    }

    public void encerrar() {
        ativo = false;
        try {
            if (servidor != null)
                servidor.close();
        } catch (IOException ignored) {
            System.out.println(ignored.getMessage());
        }
        try {
            canal.close();
        } catch (IOException ignored) {
            System.out.println(ignored.getMessage());
        }
        try {
            if (temp != null) Files.deleteIfExists(temp);
        } catch (IOException ignored) {
            System.out.println(ignored.getMessage());
        }
    }


    public static void main(String[] a) throws Exception {
        String modo = a[0];

        int porta = Integer.parseInt(a[1]);
        if (modo.equals("seed")) {
            Peer p = seed(porta, Path.of(a[2]), new Limite(mb(a[3])));
            p.iniciarServidor();
            System.out.println("Seed ouvindo na porta " + porta);
        }
        else {
            long tam = Long.parseLong(a[2]);
            List<InetSocketAddress> viz = new ArrayList<>();

            for (String hp : a[4].split(",")) {
                String[] x = hp.split(":");
                viz.add(new InetSocketAddress(x[0], Integer.parseInt(x[1])));
            }

            Peer p = leecher(porta, tam, new Limite(mb(a[3])), viz);
            p.iniciarServidor();
            p.iniciarDownload();
            System.out.printf(Locale.US, "Concluido em %.3f s ", p.aguardar());
        }
        //  quem terminou continua servindo blocos aos demais
        Thread.currentThread().join();

    }

    private static long mb(String s) {

        return (long) (Double.parseDouble(s) * 1024 * 1024);
    }
}