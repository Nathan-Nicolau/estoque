package nathan.pedidos.api.estoque.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class PedidoStatusEvent {

    private Long pedidoId;
    private String status;
    private String mensagem;
    private String data; //LocalDateTime convertido

}
