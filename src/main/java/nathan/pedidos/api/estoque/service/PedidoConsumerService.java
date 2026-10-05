package nathan.pedidos.api.estoque.service;

import jakarta.transaction.Transactional;
import nathan.pedidos.api.estoque.exception.EstoqueInsuficienteException;
import nathan.pedidos.api.estoque.exception.ProdutoNaoEncontradoException;
import nathan.pedidos.api.estoque.model.PedidoEvent;
import nathan.pedidos.api.estoque.model.PedidoStatusEvent;
import nathan.pedidos.api.estoque.model.Produto;
import nathan.pedidos.api.estoque.repository.ProdutoRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.BackOff;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Service
public class PedidoConsumerService {

    // Formatador padrão de data e hora para os eventos
    private static final DateTimeFormatter FORMATADOR_DATA = DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss");

    // Configuramos dois objetos de Logger distintos para permitir que registros normais e DLT fiquem em arquivos próprios
    private static final Logger log = LoggerFactory.getLogger(PedidoConsumerService.class);
    private static final Logger dltLog = LoggerFactory.getLogger("dlt-logger");

    // KafkaTemplate utilizado para o ciclo de feedback (notificar a Frente sobre aprovação ou reprovação)
    private final KafkaTemplate<String, PedidoStatusEvent> kafkaTemplate;

    private static final String topicoStatus = "pedidos-status";

    private final ProdutoRepository produtoRepository;

    public PedidoConsumerService(KafkaTemplate<String, PedidoStatusEvent> kafkaTemplate, ProdutoRepository produtoRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.produtoRepository = produtoRepository;
    }

    /**
     * Ciclo de Processamento e Tratamento de Resiliência:
     * 1. Tentativas (attempts = "3"): Falhas de estoque insuficiente sofrem retentativa com backoff.
     * 2. Exclusão (exclude): ProdutoNaoEncontradoException é NÃO RECUPERÁVEL. Se o produto não existe,
     *    o Spring Kafka NÃO gasta 3 tentativas inúteis; envia imediatamente para a DLT.
     * 3. DLT Handler: Captura o motivo real da exceção via cabeçalho do Kafka e notifica a Frente.
     */
    @RetryableTopic(
            attempts = "3",
            backOff = @BackOff(delay = 2000, multiplier = 1.5),
            autoCreateTopics = "true",
            exclude = { ProdutoNaoEncontradoException.class }
    )
    @Transactional
    @KafkaListener(topics = "pedidos-criados", groupId = "grupo-estoque") //O groupId aqui serve para o agrupamento lógico dos consumidores (quando houver mais de 1 instância)
    public void processarPedido(PedidoEvent pedido) {
        log.info("📥 Recebendo pedido via Kafka: {} (ID: {})", pedido.getDescricao(), pedido.getPedidoId());

        // Busca o produto no banco pelo ID vindo do evento
        Produto produto = produtoRepository.findById(pedido.getProdutoId())
                .orElseThrow(() -> {
                    String erro = "Produto não encontrado no estoque para o ID: " + pedido.getProdutoId();
                    log.warn("⚠️ {}", erro);
                    // Lança exceção específica que não sofrerá retentativas desnecessárias
                    return new ProdutoNaoEncontradoException(erro);
                });

        // Verifica se há estoque suficiente
        if (produto.getQuantidadeDisponivel() >= pedido.getQuantidade()) {
            produto.setQuantidadeDisponivel(produto.getQuantidadeDisponivel() - pedido.getQuantidade());
            produtoRepository.save(produto);
            log.info("✅ Estoque atualizado com sucesso. Produto: {}, Nova quantidade: {}", produto.getNome(), produto.getQuantidadeDisponivel());
            enviarConfirmacaoPedido(pedido);
        } else {
            String erroEstoque = "Estoque insuficiente para o produto: " + produto.getNome() + " (Disponível: " + produto.getQuantidadeDisponivel() + ", Solicitado: " + pedido.getQuantidade() + ")";
            log.warn("❌ {}", erroEstoque);
            // Lança exceção de negócio para estoque insuficiente
            throw new EstoqueInsuficienteException(erroEstoque);
        }
    }

    public void enviarConfirmacaoPedido(PedidoEvent pedido) {
        log.info("Pedido realizado com sucesso: {}", pedido.getPedidoId());

        String dataFormatada = LocalDateTime.now().format(FORMATADOR_DATA);
        var status = new PedidoStatusEvent(pedido.getPedidoId(), "APROVADO", "Pedido aprovado com sucesso", dataFormatada);

        String chaveMessage = UUID.randomUUID().toString();
        kafkaTemplate.send(topicoStatus, chaveMessage, status);
    }

    // Processamento e auditoria de mensagens que esgotaram todas as retentativas ou caíram direto na DLT
    @DltHandler
    public void processarPedidosCriadosDlt(
            PedidoEvent pedido,
            @Header(value = KafkaHeaders.EXCEPTION_MESSAGE, required = false) String mensagemErro) { // O uso do Header aqui permite a captura de metadados relacionados a mensangem. Nesse caso, a exception em si

        // O Spring Kafka injeta o motivo exato lançado na exceção através do cabeçalho da DLT
        String motivoReal = (mensagemErro != null && !mensagemErro.isBlank())
                ? mensagemErro
                : "Falha definitiva no processamento do pedido";

        String logDetalhado = "🚨 [DLQ] Pedido rejeitado definitivamente. Motivo: " + motivoReal +
                " | Pedido: " + pedido.getDescricao() +
                " | ID Pedido: " + pedido.getPedidoId() +
                " | Produto ID: " + pedido.getProdutoId() +
                " | Qtd: " + pedido.getQuantidade();
        
        // Registra no arquivo específico da DLT
        dltLog.error(logDetalhado);
        
        // Registra também no log geral da aplicação
        log.error(logDetalhado);

        String dataFormatada = LocalDateTime.now().format(FORMATADOR_DATA);

        // Envia o feedback à Frente com o status REPROVADO e o motivo real (produto inexistente ou estoque insuficiente)
        var pedidoStatusReprovado = new PedidoStatusEvent(
                pedido.getPedidoId(),
                "REPROVADO",
                motivoReal,
                dataFormatada
        );
        kafkaTemplate.send(topicoStatus, pedido.getPedidoId().toString(), pedidoStatusReprovado);
    }


}
