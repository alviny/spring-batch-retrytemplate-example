package example;

import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Example configuration class that Spring utilizes to generate bean definitions.
 * For production projects, consider using a different a database backed job repository. 
 * See {@link MapJobRepositoryFactoryBean} for limitations.
 * 
 * @author Levi Hinze
 * 
 */
@Configuration
public class ExampleConfiguration {
	
	@Bean
	ResourcelessTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}
	
	@Bean
	MapJobRepositoryFactoryBean jobRepository() {
		MapJobRepositoryFactoryBean repository = new MapJobRepositoryFactoryBean();
		repository.setTransactionManager(transactionManager());
		return repository;
	}
	
	@Bean
	SimpleJobLauncher jobLauncher() throws Exception {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository().getJobRepository());
		return launcher;
	}
	
}
