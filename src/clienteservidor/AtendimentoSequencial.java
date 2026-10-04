package clienteservidor;

public class AtendimentoSequencial implements Atendimento {
    @Override
    public void despachar(Runnable tarefa) {
        // o próximo cliente só é aceito quando este termina
        tarefa.run();
    }

    @Override
    public void encerrar() { }
}