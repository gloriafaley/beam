@EnableBinding(Channels.class)
@SpringBootApplication
public class PubsubPublisherApplication {
  public static void main(String[] args) {
    SpringApplication.run(PubsubPublisherApplication.class, args);
  }
}
