package clienteservidor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class AtendimentoPool implements Atendimento {

    private final ExecutorService pool;

    public AtendimentoPool(int n) {
        // no maximo n downloads simultaneos o resto espera na fila interna do pool
        this.pool = Executors.newFixedThreadPool(n);
    }

    @Override
    public void despachar(Runnable tarefa) {
        pool.submit(tarefa);
    }

    @Override
    public void encerrar() {
        pool.shutdownNow();
    }
}