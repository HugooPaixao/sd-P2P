package clienteservidor;

import comum.Limite;
import java.io.*;
import java.net.*;

public class Servidor {
    private static final int BLOCO = 32 * 1024;     // tamanho de cada escrita

    private final int porta;
    private final File arquivo;
    private final Limite limite;
    private final Atendimento atendimento;
    private ServerSocket ss;
    private Thread aceitador;

    private Servidor(int porta, File arquivo, Limite limite, Atendimento atendimento) {
        this.porta = porta;
        this.arquivo = arquivo;
        this.limite = limite;
        this.atendimento = atendimento;
    }

    public static Servidor criar(String tipo, int porta, File arq, Limite lim, int nPool) {

        Atendimento a = switch (tipo) {
            case "sequencial" -> new AtendimentoSequencial();
            case "threads"    -> new AtendimentoThreads();
            case "pool"       -> new AtendimentoPool(nPool);
            default -> throw new IllegalArgumentException("Tipo desconhecido: " + tipo);
        };

        return new Servidor(porta, arq, lim, a);
    }

    public void iniciar() throws IOException {

        ss = new ServerSocket();
        ss.setReuseAddress(true); // reabrir a mesma porta entre experimentos seguidos
        ss.bind(new InetSocketAddress(porta), 1000);

        aceitador = new Thread(() -> {

            while (!ss.isClosed()) {
                try {
                    Socket s = ss.accept();
                    atendimento.despachar(() -> atender(s));
                } catch (IOException e) {
                    System.out.println(e.getMessage());
                    break;
                }
            }
        }, "aceitador-" + porta);
        aceitador.start();
    }

    public void parar() throws IOException, InterruptedException {
        ss.close();
        aceitador.join();
        atendimento.encerrar();
    }

    void atender(Socket s) {
        try (s; FileInputStream in = new FileInputStream(arquivo)) {
            OutputStream out = s.getOutputStream();

            // O tamanho vai antes para o cliente saber quando parar de ler
            new DataOutputStream(out).writeLong(arquivo.length());
            byte[] buf = new byte[BLOCO];
            int n;
            while ((n = in.read(buf)) > 0) {
                limite.consumir(n);              // reserva banda antes de enviar
                out.write(buf, 0, n);
            }
            out.flush();
        } catch (IOException e) {
            System.err.println("Falha ao atender cliente: " + e.getMessage());
        }
    }
}