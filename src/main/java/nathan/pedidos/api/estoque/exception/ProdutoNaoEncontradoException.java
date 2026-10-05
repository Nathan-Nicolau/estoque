package nathan.pedidos.api.estoque.exception;

/**
 * Exceção de Negócio: Lançada quando o produto solicitado não existe no catálogo.
 * <p>
 * Essa é uma exceção NÃO RECUPERÁVEL (Non-Retryable). Se o produto não existe,
 * retentativas imediatas do Kafka não resolverão o problema.
 * Por isso, configuramos o @RetryableTopic para ignorar o retry e enviar direto para a DLT.
 */
public class ProdutoNaoEncontradoException extends RuntimeException {

    public ProdutoNaoEncontradoException(String message) {
        super(message);
    }
}
