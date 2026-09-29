package nathan.pedidos.api.estoque;

import nathan.pedidos.api.estoque.model.Produto;
import nathan.pedidos.api.estoque.repository.ProdutoRepository;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class EstoqueApplication {

	public static void main(String[] args) {
		SpringApplication.run(EstoqueApplication.class, args);
	}

	// Este Bean é executado automaticamente durante o startup
	@Bean
	public CommandLineRunner initDatabase(ProdutoRepository repository) {
		return args -> {
			System.out.println("⚙️ Inicializando banco de dados com produtos de teste...");

			// Insere o produto 105 com estoque inicial de 10 unidades sempre que a API é inicializada
			// Usando banco H2 em memória
			repository.save(new Produto(105L, "Monitor Gamer 144hz", 10));

			System.out.println("✅ Produto de teste inserido com sucesso!");
		};
	}

}
