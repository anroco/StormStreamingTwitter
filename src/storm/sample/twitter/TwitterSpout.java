package storm.sample.twitter;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @author anroco Este Spout permite obtener el streaming de twitter a traves
 *         del api Twitter4J, definiendo las palabras clave que se quiere hacer
 *         seguimiento.
 */
public class TwitterSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private TwitterStream twitterStream;
	private SpoutOutputCollector collector;
	private LinkedBlockingQueue<Status> queue;

	@Override
	public void open(Map conf, TopologyContext contex,
			SpoutOutputCollector collector) {
		queue = new LinkedBlockingQueue<Status>(1000);
		this.collector = collector;

		ConfigurationBuilder cb = new ConfigurationBuilder();

		// Definir los Keys and Access Tokens de la aplicacion creada en
		// Twitter.
		cb.setOAuthConsumerKey("eU9ZQCb4OrloQglOc6UTbERcm")
				.setOAuthConsumerSecret(
						"Qk6y8S39XMtxbjLrCwyBGdHZxyc3Gr5d0EqSW1OGx2mP37mtC2")
				.setOAuthAccessToken(
						"85721956-ea8eXE5H6NjPvpAPh2UsxfWsAVmdbrXhgXXCNJpCu")
				.setOAuthAccessTokenSecret(
						"OEbHgHkJxg4qWP8HmR7E5E5cPrRND1bhzbVb6hhhyHvfX");

		TwitterStream twitterStream = new TwitterStreamFactory(cb.build())
				.getInstance();
		twitterStream.addListener(new StatusListener() {
			public void onStatus(Status status) {
				queue.offer(status);
			}

			@Override
			public void onException(Exception e) {
			}

			@Override
			public void onDeletionNotice(StatusDeletionNotice sdn) {
			}

			@Override
			public void onScrubGeo(long l1, long l2) {
			}

			@Override
			public void onStallWarning(StallWarning sw) {
			}

			@Override
			public void onTrackLimitationNotice(int i) {
			}
		});

		FilterQuery tweetFilterQuery = new FilterQuery();

		// Definir las palabras clave hacer seguimiento.
		tweetFilterQuery.track(new String[] { "MovistarCo", "ClaroColombia",
				"Tigo_Colombia", "UNEMejorjuntos", "Virgin_MobileCo",
				"Uff_MOVIL" });
		twitterStream.filter(tweetFilterQuery);
	}

	@Override
	public void close() {
		twitterStream.shutdown();
	}

	@Override
	public void nextTuple() {
		Status tweet = queue.poll();
		if (tweet == null)
			Utils.sleep(50);
		else
			collector.emit(new Values(tweet));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("tweet"));
	}
}