package comum;

import java.util.concurrent.locks.LockSupport;

/**
 * Emula a banda de upload de uma placa de rede.
 * Em localhost não há gargalo de rede, então sem isso os testes mediriam só CPU/memória.
 * Uma única instância compartilhada por várias threads divide a banda entre elas.
 */
public class Limite {
    private final double nsPorByte;                 // 0 é sem limite
    private long proximo = System.nanoTime();       // instante em que o canal fica livre

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
            if (proximo < agora) {
                proximo = agora;   // não acumula  banda
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