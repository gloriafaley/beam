/*
The Beam SDKs provide a number of abstractions that simplify the mechanics of 
large-scale distributed data processing. The same Beam abstractions work with both 
batch and streaming data sources. When you create your Beam pipeline, you can think about 
your data processing task in terms of these abstractions. They include:

Pipeline: A Pipeline encapsulates your entire data processing task, from start to finish. 
This includes reading input data, transforming that data, and writing output data. 
All Beam driver programs must create a Pipeline. When you create the Pipeline, 
you must also specify the execution options that tell the Pipeline where and how to run.

PCollection: A PCollection represents a distributed data set that your Beam pipeline operates on. 
The data set can be bounded, meaning it comes from a fixed source like a file, or unbounded, meaning 
it comes from a continuously updating source via a subscription or other mechanism. Your pipeline 
typically creates an initial PCollection by reading data from an external data source, but you can also
create a PCollection from in-memory data within your driver program. From there, PCollections are the inputs 
and outputs for each step in your pipeline.

PTransform: A PTransform represents a data processing operation, or a step, in your pipeline. 
Every PTransform takes one or more PCollection objects as input, performs a processing function that you 
rovide on the elements of that PCollection, and produces zero or more output PCollection objects.

I/O transforms: Beam comes with a number of “IOs” - library PTransforms that read or write data to 
various external storage systems.
*/


import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableReference;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.PipelineResult;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.CreateDisposition;
import com.google.cloud.dataflow.sdk.io.BigQueryIO.Write.WriteDisposition;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.StreamingOptions;
import com.google.cloud.dataflow.sdk.runners.DataflowPipelineRunner;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.DoFn.RequiresWindowAccess;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.windowing.IntervalWindow;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.tutorials.game.solutions.Exercise1;
import com.google.cloud.dataflow.tutorials.game.solutions.Exercise3;
import com.google.cloud.dataflow.tutorials.game.utils.GameEvent;
import com.google.cloud.dataflow.tutorials.game.utils.Options;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import org.joda.time.Duration;
import org.joda.time.Instant;

/**
 * Fourth in a series of coding exercises in a gaming domain.
 *
 * <p>This streaming pipeline calculates user and team scores for a window of time and writes them
 * to BigQuery.
 *
 * <p>See README.md for details.
 */
public class Exercise4 {

  static final Duration TEN_SECONDS = Duration.standardSeconds(10);
  static final Duration THIRTY_SECONDS = Duration.standardSeconds(30);

  /** Exercise4Options supported by {@link Exercise4}. */
  interface Exercise4Options extends Options, StreamingOptions {
    @Description("Numeric value of fixed window duration for team analysis, in minutes")
    @Default.Integer(1)
    Integer getTeamWindowDuration();

    void setTeamWindowDuration(Integer value);

    @Description("Numeric value of allowed data lateness, in minutes")
    @Default.Integer(2)
    Integer getAllowedLateness();

    void setAllowedLateness(Integer value);
  }

  /**
   * Extract user/score pairs from the event stream using processing time, via global windowing. Get
   * periodic updates on all users' running scores.
   */
  @VisibleForTesting
  static class CalculateUserScores
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {
    private final Duration allowedLateness;

    CalculateUserScores(Duration allowedLateness) {
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<GameEvent> input) {
      // [START EXERCISE 4 PART 1]:
      // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
      // Developer Docs: https://cloud.google.com/dataflow/model/par-do
      //
      // Fill in the code to:
      //   1. Window the incoming input into global windows
      //   2. trigger every thirty seconds to emit speculative results.
      return input
          /* TODO: SOLUTION CODE HERE */
          // Extract and sum username/score pairs from the event data.
          .apply("ExtractUserScore", new Exercise1.ExtractAndSumScore("user"));
      // [END EXERCISE 4 PART 1]:
    }
  }

  /** Calculates scores for each team within the configured window duration. */
  // Extract team/score pairs from the event stream, using hour-long windows by default.
  @VisibleForTesting
  static class CalculateTeamScores
      extends PTransform<PCollection<GameEvent>, PCollection<KV<String, Integer>>> {
    private final Duration teamWindowDuration;
    private final Duration allowedLateness;

    CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
      this.teamWindowDuration = teamWindowDuration;
      this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> apply(PCollection<GameEvent> infos) {
      // [START EXERCISE 4 PART 2]:
      // JavaDoc: https://cloud.google.com/dataflow/java-sdk/JavaDoc
      // Developer Docs: https://cloud.google.com/dataflow/model/par-do
      //
      // Fill in the code to:
      //   1. Window the incoming input into fixed windows of team window duration
      //   2. trigger on time results at the watermark
      //   3. trigger speculative results every ten seconds
      //   4. trigger late data results with a delay of thirty seconds
      return infos
          /* TODO: SOLUTION CODE HERE */
          // Extract and sum teamname/score pairs from the event data.
          .apply("ExtractTeamScore", new Exercise1.ExtractAndSumScore("team"));
      // [END EXERCISE 4 PART 2]:
    }
  }

  public static void main(String[] args) throws Exception {
    Exercise4Options options =
        PipelineOptionsFactory.fromArgs(args).withValidation().as(Exercise4Options.class);
    // Enforce that this pipeline is always run in streaming mode.
    options.setStreaming(true);
    // For example purposes, allow the pipeline to be easily cancelled instead of running
    // continuously.
    options.setRunner(DataflowPipelineRunner.class);
    Pipeline pipeline = Pipeline.create(options);

    TableReference teamTable = new TableReference();
    teamTable.setDatasetId(options.getOutputDataset());
    teamTable.setProjectId(options.getProject());
    teamTable.setTableId(options.getOutputTableName() + "_team");

    TableReference userTable = new TableReference();
    userTable.setDatasetId(options.getOutputDataset());
    userTable.setProjectId(options.getProject());
    userTable.setTableId(options.getOutputTableName() + "_user");

    PCollection<GameEvent> gameEvents = pipeline.apply(new Exercise3.ReadGameEvents(options));

    gameEvents
        .apply(
            "CalculateTeamScores",
            new CalculateTeamScores(
                Duration.standardMinutes(options.getTeamWindowDuration()),
                Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(ParDo.named("FormatTeamScores").of(new FormatTeamScoreFn()))
        .apply(
            BigQueryIO.Write.to(teamTable)
                .withSchema(FormatTeamScoreFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    gameEvents
        .apply(
            "CalculateUserScores",
            new CalculateUserScores(Duration.standardMinutes(options.getAllowedLateness())))
        // Write the results to BigQuery.
        .apply(ParDo.named("FormatUserScores").of(new FormatUserScoreFn()))
        .apply(
            BigQueryIO.Write.to(userTable)
                .withSchema(FormatUserScoreFn.getSchema())
                .withCreateDisposition(CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(WriteDisposition.WRITE_APPEND));

    // Run the pipeline and wait for the pipeline to finish; capture cancellation requests from the
    // command line.
    PipelineResult result = pipeline.run();
  }

  /** Format a KV of team and associated properties to a BigQuery TableRow. */
  protected static class FormatTeamScoreFn extends DoFn<KV<String, Integer>, TableRow>
      implements RequiresWindowAccess {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("team", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("window_start", ((IntervalWindow) c.window()).start().getMillis() / 1000)
              .set("processing_time", Instant.now().getMillis() / 1000)
              .set("timing", c.pane().getTiming().toString());
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("team").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("window_start").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
      fields.add(new TableFieldSchema().setName("timing").setType("STRING"));
      return new TableSchema().setFields(fields);
    }
  }

  /** Format a KV of user and associated properties to a BigQuery TableRow. */
  static class FormatUserScoreFn extends DoFn<KV<String, Integer>, TableRow> {
    @Override
    public void processElement(ProcessContext c) {
      TableRow row =
          new TableRow()
              .set("user", c.element().getKey())
              .set("total_score", c.element().getValue())
              .set("processing_time", Instant.now().getMillis() / 1000);
      c.output(row);
    }

    static TableSchema getSchema() {
      List<TableFieldSchema> fields = new ArrayList<>();
      fields.add(new TableFieldSchema().setName("user").setType("STRING"));
      fields.add(new TableFieldSchema().setName("total_score").setType("INTEGER"));
      fields.add(new TableFieldSchema().setName("processing_time").setType("TIMESTAMP"));
      return new TableSchema().setFields(fields);
    }
  }
}
