package nathan.pedidos.api.estoque.model;

import lombok.Getter;
import lombok.Setter;
import java.math.BigDecimal;

@Getter
@Setter
public class PedidoEvent {
    private Long pedidoId;
    private Long produtoId;
    private String descricao;
    private BigDecimal valor;
    private int quantidade;
}