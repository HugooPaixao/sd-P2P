package clienteservidor;

// Define como as tarefas de atendimento são executadas
public interface Atendimento {
    void despachar(Runnable tarefa);

    // liberar recursos ao parar o servidor
    void encerrar();
}
