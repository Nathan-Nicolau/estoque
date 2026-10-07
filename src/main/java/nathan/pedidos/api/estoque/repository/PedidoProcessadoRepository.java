package nathan.pedidos.api.estoque.repository;

import nathan.pedidos.api.estoque.model.PedidoProcessado;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface PedidoProcessadoRepository extends JpaRepository<PedidoProcessado, Long> {}
