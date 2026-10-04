package comum;

import java.util.concurrent.locks.LockSupport;


public class Limite {
    private final double nsPorByte;                 // 0 é sem limite
    private long proximo = System.nanoTime();       // instante em que o canal fica livre
    private static final long TOLERANCIA_NS = 2_000_000;

    public Limite(long bytesPorSegundo) {
        this.nsPorByte = bytesPorSegundo <= 0 ? 0 : 1_000_000_000.0 / bytesPorSegundo;
    }



    public void consumir(int bytes) {
        if (nsPorByte == 0) {
            return;
        }

        long alvo;

        synchronized (this) {
            long agora = System.nanoTime();
            if (proximo < agora - TOLERANCIA_NS) {
                proximo = agora - TOLERANCIA_NS;
            }

            proximo += (long) (bytes * nsPorByte);
            alvo = proximo;
        }
        long resta;

        while ((resta = alvo - System.nanoTime()) > 0) {
            LockSupport.parkNanos(resta);
        }


    }



}