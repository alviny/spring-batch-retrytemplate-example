package example;

import java.util.Random;
import org.apache.log4j.Logger;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

/**
 * An example tasklet where we want to retry an operation if the failure meets criteria 
 * supplied by the {@link RetryTemplate} object.
 * This is a fairly simple example and one could argue that using the RetryTemplate is
 * overkill, but more complex scenarios could benefit from this method.
 * 
 * @author Levi Hinze
 *
 */
@Component("errorProneTask")
public class ErrorProneTask implements Tasklet {
	private static final Logger log = Logger.getLogger(ErrorProneTask.class);
	
	
	private RetryTemplate retryTemplate;
	
	private int maxInt;
	
	/**
	 * We create the RetryTemplate bean in the batch context, which allows us to reuse if needed.
	 * However, we could be create it within this class if we need a specialized RetryTemplate.
	 */
	@Required
	@Autowired
	@Qualifier("retryTemplate")
	public void setRetryTemplate(RetryTemplate retryTemplate) {
		this.retryTemplate = retryTemplate;
	}
	
	@Required
	@Value("${batch.example.maxint}")
	public void setMaxInt(int maxInt) {
		this.maxInt = maxInt;
	}

	@Override
	public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
		Assert.isTrue(maxInt > 0, "maxInt must be greater than 0.");
		
		return retryTemplate.execute(new RetryCallback<RepeatStatus>() {
			public RepeatStatus doWithRetry(RetryContext context) throws Exception {
				log.debug("Attempt number " + context.getRetryCount());
				
				// Let's do something that can fail
				Random generator = new Random();
				int randomNum = generator.nextInt(maxInt);
				
				// Theoretically, this should fail roughly 50% of the time
				if (randomNum < Math.floor(maxInt / 2) ) {
					log.debug("I failed the test!");
					throw new RetryException();
				}
				
				log.debug("Finished!");
				return RepeatStatus.FINISHED;
			}
		});
	}
}
