package experimento;

import clienteservidor.ClienteDownload;
import clienteservidor.Servidor;
import comum.Limite;
import p2p.Peer;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.*;

public class Experimentos {
    static final String HOST = "127.0.0.1";
    static final int PORTA_CS = 9000;
    static final int PORTA_P2P = 10000;

    public static void main(String[] args) throws Exception {

        double taxaMBps = args.length > 0 ? Double.parseDouble(args[0]) : 50;
        int repeticoes  = args.length > 1 ? Integer.parseInt(args[1]) : 3;

        int[] tamanhos  = lista(args.length > 2 ? args[2] : "5,50,500");
        int[] clientes  = lista(args.length > 3 ? args[3] : "1,5,10,20");
        int nPool       = args.length > 4 ? Integer.parseInt(args[4]) : 4;
        String prefixo = args.length > 5 ? args[5] : "";

        long bps = (long)(taxaMBps * 1024 * 1024);

        String[] arquiteturas = {"sequencial", "threads", "pool", "p2p"};
        Files.createDirectories(Path.of("resultados"));

        try (PrintWriter bruto = new PrintWriter(new FileWriter("resultados/" + prefixo + "tempos_brutos.csv"));
             PrintWriter resumo = new PrintWriter(new FileWriter("resultados/" + prefixo + "resumo.csv"))) {

            bruto.println("arquitetura,tamanhoemMB,clientes,repeticao,cliente,tempoSegundos");
            resumo.println("arquitetura,tamanhoemMB,clientes,minimo,medio,maximo");

            for (int tam : tamanhos) {
                File arquivo = new File("arquivos/arquivo_" + tam + "MB.bin");
                if (!arquivo.exists()) {
                    throw new FileNotFoundException("Rode GeradorArquivos antes: " + arquivo);
                }

                for (int k : clientes) {
                    for (String arq : arquiteturas) {
                        List<Double> todos = new ArrayList<>();
                        for (int r = 1; r <= repeticoes; r++) {
                            double[] t = arq.equals("p2p")
                                    ? executarP2P(arquivo, k, bps)
                                    : executarCS(arq, arquivo, k, bps, nPool);

                            for (int c = 0; c < t.length; c++) {
                                bruto.printf(Locale.US, "%s,%d,%d,%d,%d,%.4f%n", arq, tam, k, r, c + 1, t[c]);
                                todos.add(t[c]);
                            }

                            System.out.printf(Locale.US, "[%s] %d MB, %d clientes, rodada %d/%d concluída%n",
                                    arq, tam, k, r, repeticoes);
                        }

                        DoubleSummaryStatistics st = todos.stream().mapToDouble(d -> d).summaryStatistics();
                        resumo.printf(Locale.US, "%s,%d,%d,%.4f,%.4f,%.4f%n",
                                arq, tam, k, st.getMin(), st.getAverage(), st.getMax());

                        resumo.flush();
                        bruto.flush();
                    }
                }
            }
        }
        System.out.println("Pronto! Veja a pasta resultados/");
    }

    public static double[] executarCS(String tipo, File arquivo, int k, long bps, int nPool) throws Exception {
        // liite novo a cada execução
        Servidor s = Servidor.criar(tipo, PORTA_CS, arquivo, new Limite(bps), nPool);
        s.iniciar();
        try {
            return ClienteDownload.baixarEmParalelo(HOST, PORTA_CS, k);
        } finally {
            s.parar();
        }
    }

    public static double[] executarP2P(File arquivo, int k, long bps) throws Exception {
        long tamanho = arquivo.length();
        List<InetSocketAddress> enderecos = new ArrayList<>();
        for (int i = 0; i <= k; i++) {
            enderecos.add(new InetSocketAddress(HOST, PORTA_P2P + i));
        }

        List<Peer> todos = new ArrayList<>();
        try {
            Peer seed = Peer.seed(PORTA_P2P, arquivo.toPath(), new Limite(bps));
            seed.iniciarServidor();
            todos.add(seed);

            List<Peer> leechers = new ArrayList<>();
            for (int i = 1; i <= k; i++) {
                List<InetSocketAddress> vizinhos = new ArrayList<>(enderecos);
                vizinhos.remove(i);                 // índice i = o próprio peer; não pedir blocos a si mesmo

                Peer p = Peer.leecher(PORTA_P2P + i, tamanho, new Limite(bps), vizinhos);
                p.iniciarServidor();
                leechers.add(p);
                todos.add(p);
            }
            // Só incia os downloads depois de todos os servidres estarem no ar
            for (Peer p : leechers) {
                p.iniciarDownload();
            }

            double[] tempos = new double[k];
            for (int i = 0; i < k; i++) {
                tempos[i] = leechers.get(i).aguardar();
            }

            return tempos;
        } finally {
            for (Peer p : todos) {
                p.encerrar();      // fecha portas e apaga os temporários
            }
        }
    }

    public static int[] lista(String s) {
        return Arrays.stream(s.split(",")).mapToInt(Integer::parseInt).toArray();
    }
}