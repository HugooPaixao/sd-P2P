package clienteservidor;

import java.io.*;
import java.net.Socket;
import java.util.Arrays;
import java.util.DoubleSummaryStatistics;
import java.util.Locale;
import java.util.concurrent.CyclicBarrier;

public class ClienteDownload {

    // faz 1 download e devolve o tempo em segundos
    public static double baixar(String host, int porta) throws IOException {
        // no servidor sequencial/pool, o tempo de espera

        long t0 = System.nanoTime();
        try (Socket s = new Socket(host, porta)) {

            DataInputStream in = new DataInputStream(s.getInputStream());
            long total = in.readLong();
            byte[] buf = new byte[64 * 1024];
            long lido = 0;
            while (lido < total) {
                int n = in.read(buf, 0, (int) Math.min(buf.length, total - lido));
                if (n < 0) {
                    throw new EOFException("Conexão fechada antes do fim");
                }
                lido += n;
            }
        }
        return (System.nanoTime() - t0) / 1e9;
    }

    // Dispara n clientes simultâneos e devolve o tempo de cada um
    public static double[] baixarEmParalelo(String host, int porta, int n) throws Exception {
        double[] tempos = new double[n];

        CyclicBarrier barreira = new CyclicBarrier(n);
        Thread[] ts = new Thread[n];
        for (int i = 0; i < n; i++) {
            final int id = i;

            ts[i] = new Thread(() -> {
                try {
                    barreira.await();
                    tempos[id] = baixar(host, porta);
                } catch (Exception e) {
                    tempos[id] = Double.NaN;
                    e.printStackTrace();
                }
            });

            ts[i].start();
        }
        for (Thread t : ts) {
            t.join();
        }

        return tempos;
    }

    public static void main(String[] a) throws Exception {

        double[] t = baixarEmParalelo(a[0], Integer.parseInt(a[1]), Integer.parseInt(a[2]));
        System.out.println("Tempos: " + Arrays.toString(t));
        DoubleSummaryStatistics st = Arrays.stream(t).summaryStatistics();

        System.out.printf(Locale.US, "min=%.3f  medio=%.3f  max=%.3f%n",
                st.getMin(), st.getAverage(), st.getMax());
    }
}