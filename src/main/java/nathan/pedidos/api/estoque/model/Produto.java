package nathan.pedidos.api.estoque.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "PRODUTO")
@Getter
@Setter
public class Produto {

    @Id
    private Long id;
    private String nome;
    private int quantidadeDisponivel;

    public Produto() {}

    public Produto(Long id, String nome, int quantidadeDisponivel) {
        this.id = id;
        this.nome = nome;
        this.quantidadeDisponivel = quantidadeDisponivel;
    }
}
