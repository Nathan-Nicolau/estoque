package nathan.pedidos.api.estoque.exception;

/**
 * Exceção de Negócio: Lançada quando a quantidade em estoque é menor que a solicitada.
 * <p>
 * Essa falha pode ser tratada com retry caso haja reposição rápida ou bloqueio temporário,
 * ou enviada para a DLT para descarte e notificação de reprovação.
 */
public class EstoqueInsuficienteException extends RuntimeException {

    public EstoqueInsuficienteException(String message) {
        super(message);
    }
}
