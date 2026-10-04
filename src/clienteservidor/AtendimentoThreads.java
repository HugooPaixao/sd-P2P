package clienteservidor;

public class AtendimentoThreads implements Atendimento {
    @Override
    public void despachar(Runnable tarefa) {
        // uma thread por cliente
        new Thread(tarefa).start();
    }

    @Override
    public void encerrar() { }
}