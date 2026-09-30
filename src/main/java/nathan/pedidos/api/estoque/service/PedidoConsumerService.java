package nathan.pedidos.api.estoque.service;

import jakarta.transaction.Transactional;
import nathan.pedidos.api.estoque.model.PedidoEvent;
import nathan.pedidos.api.estoque.model.Produto;
import nathan.pedidos.api.estoque.repository.ProdutoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.stereotype.Service;

@Service
public class PedidoConsumerService {

    //Configuramos dois objetos de Logger distintos aqui para permitir que os registros tradicionais e erros sejam salvos em arquivos de Logs próprios
    private static final Logger log = LoggerFactory.getLogger(PedidoConsumerService.class);
    private static final Logger dltLog = LoggerFactory.getLogger("dlt-logger");

    private final ProdutoRepository produtoRepository;

    public PedidoConsumerService(ProdutoRepository produtoRepository) {
        this.produtoRepository = produtoRepository;
    }

    /**
     * Explicação do processo de envio de mensagem ao DLQ
     * <p>
     *   <b>1. A Falha no Consumo:</b>
     *   O método @KafkaListener lê a mensagem do tópico original (pedidos-criados). Ao identificar que não há estoque suficiente, o seu código lança uma RuntimeException.
     *   <p>
     *   <b>2. O Ciclo de Retentativas (Retry):</b>
     *   A anotação @RetryableTopic intercepta essa exceção. Em vez de descartar a mensagem ou travar a fila, ela suspende temporariamente e repete o processamento respeitando o @BackOff (espera 2s na 2ª
     *   tentativa e 3s na 3ª tentativa).
     *   <p>
     *   <b>3. A Publicação Física na DLT:</b>
     *   Ao falhar na 3ª tentativa, o Spring Kafka desiste do tópico principal, comita o offset dele para não travar os próximos pedidos e publica a mensagem via rede em um novo tópico no broker Kafka: pedidos-
     *   criados-dlt, incluindo cabeçalhos com o motivo do erro e o stacktrace.
     *   <p>
     *   <b>4. O Consumo da DLT:</b>
     *   Durante a inicialização da aplicação, o Spring cria nos bastidores um segundo consumidor Kafka dedicado exclusivamente a monitorar o tópico pedidos-criados-dlt. Assim que a mensagem chega nele, esse
     *   consumidor a lê e entrega para o método anotado com @DltHandler.
     *   <p>
     *   <b>5. A Gravação no Arquivo de Erro:</b>
     *   O @DltHandler executa e envia o alerta para o logger dlt-logger. O logback-spring.xml reconhece esse identificador e grava a mensagem isolada no arquivo .txt do dia (erros-pedidos-criados-dlt-29-09-26.
     *   txt).
     *
     * */

    // Temos o uso do RetryableTopic aqui para casos onde ocorrerem erros de processamento das mensagens
    // 3 tentativas ao total
    // Configuração de delay com o backoff
    // Permissão para a criação do tópico de "erros" automaticamente se não existir
    // Por padrão, um tópico de dlt é nomeado seguindo a convenção do tópico atual - nesse caso, será pedidos-criados-dlt
    @RetryableTopic(attempts = "3", backOff = @BackOff(delay = 2000, multiplier = 1.5), autoCreateTopics = "true")
    @Transactional
    // Essa é a função que é chamada quando o Kafka lê o tópico configurado aqui com o Listener
    @KafkaListener(topics = "pedidos-criados", groupId = "grupo-estoque")
    public void processarPedido(PedidoEvent pedido) {
        log.info("📥 Recebendo pedido via Kafka: {} (ID: {})", pedido.getDescricao(), pedido.getPedidoId());

        // Busca o produto no banco pelo ID vindo do evento
        Produto produto = produtoRepository.findById(pedido.getProdutoId())
                .orElseThrow(() -> {
                    String erro = "Produto não encontrado no estoque para o ID: " + pedido.getProdutoId();
                    log.warn("⚠️ {}", erro);
                    return new RuntimeException(erro);
                });

        // Verifica se há estoque suficiente
        if (produto.getQuantidadeDisponivel() >= pedido.getQuantidade()) {
            produto.setQuantidadeDisponivel(produto.getQuantidadeDisponivel() - pedido.getQuantidade());
            produtoRepository.save(produto);
            log.info("✅ Estoque atualizado com sucesso. Produto: {}, Nova quantidade: {}", produto.getNome(), produto.getQuantidadeDisponivel());
        } else {
            String erroEstoque = "Estoque insuficiente para o produto: " + produto.getNome() + " (Disponível: " + produto.getQuantidadeDisponivel() + ", Solicitado: " + pedido.getQuantidade() + ")";
            log.warn("❌ {}", erroEstoque);
            throw new RuntimeException(erroEstoque);
        }
    }

    // Processamento e auditoria de mensagens que esgotaram todas as retentativas e caíram na DLT
    @DltHandler
    public void processarPedidosCriadosDlt(PedidoEvent pedido) {
        String mensagemErro = "🚨 [DLQ] Pedido rejeitado definitivamente após tentativas esgotadas. Pedido: " + pedido.getDescricao() + " | ID Pedido: " + pedido.getPedidoId() + " | Produto ID: " + pedido.getProdutoId() + " | Qtd: " + pedido.getQuantidade();
        
        // Registra no arquivo específico da DLT
        dltLog.error(mensagemErro);
        
        // Registra também no log geral da aplicação
        log.error(mensagemErro);
    }


}
