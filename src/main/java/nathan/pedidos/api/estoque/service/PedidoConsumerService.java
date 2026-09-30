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
