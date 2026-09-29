package nathan.pedidos.api.estoque.service;

import jakarta.transaction.Transactional;
import nathan.pedidos.api.estoque.model.PedidoEvent;
import nathan.pedidos.api.estoque.model.Produto;
import nathan.pedidos.api.estoque.repository.ProdutoRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class PedidoConsumerService {

    private final ProdutoRepository produtoRepository;

    public PedidoConsumerService(ProdutoRepository produtoRepository) {
        this.produtoRepository = produtoRepository;
    }

    // Essa é a função que é chamada quando o Kafka lê o tópico configurado aqui com o Listener
    @Transactional
    @KafkaListener(topics = "pedidos-criados", groupId = "grupo-estoque")
    public void processarPedido(PedidoEvent pedido) {
        System.out.println("📥 Recebendo pedido via Kafka: " + pedido.getDescricao());

        // Busca o produto no banco pelo ID vindo do evento
        Produto produto = produtoRepository.findById(pedido.getProdutoId())
                .orElseThrow(() -> new RuntimeException("Produto não encontrado no estoque!"));

        // Verifica se há estoque suficiente
        if (produto.getQuantidadeDisponivel() >= pedido.getQuantidade()) {
            produto.setQuantidadeDisponivel(produto.getQuantidadeDisponivel() - pedido.getQuantidade());
            produtoRepository.save(produto);
            System.out.println("✅ Estoque atualizado. Nova quantidade: " + produto.getQuantidadeDisponivel());
        } else {
            System.out.println("❌ Estoque insuficiente para o produto: " + produto.getNome());
            // Em um cenário real, enviaríamos uma mensagem para uma DLQ ou para um tópico de "pedido-recusado"
        }
    }

}
