package clienteservidor;

import comum.Limite;
import java.io.File;

public class ServidorMain {
    public static void main(String[] a) throws Exception {
        String tipo = a[0];
        int porta = Integer.parseInt(a[1]);
        File arq = new File(a[2]);
        Limite lim = new Limite((long) (Double.parseDouble(a[3]) * 1024 * 1024));

        int n = a.length > 4 ? Integer.parseInt(a[4]) : 4;
        Servidor s = Servidor.criar(tipo, porta, arq, lim, n);
        s.iniciar();
        System.out.println("Servidor " + tipo + " na porta " + porta + " servindo " + arq);
    }
}