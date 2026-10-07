package nathan.pedidos.api.estoque.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import java.util.Date;

@Entity
@Table(name = "PEDIDO_PROCESSADO")
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class PedidoProcessado {

    @Id //Aqui para evitar possíveis duplicações a nível de banco de dados, mantemos o pedidoId como PK
    private Long pedidoId;
    private Date dataProcessamento;

}
