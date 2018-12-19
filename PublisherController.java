@RestController
public class PublisherController {
  private final MessageChannel outgoing;
  public PublisherController(Channels channels) {
    outgoing = channels.outgoing();
  }
  @PostMapping("/publish/{name}")
  public void publish(@PathVariable String name) {
    outgoing.send(MessageBuilder.withPayload("Hello " + name + "!").build());
  }
}
