package com.github.tjake.mvbench;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import javax.inject.Inject;

import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.Uninterruptibles;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.HostDistance;
import com.datastax.driver.core.PoolingOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.DowngradingConsistencyRetryPolicy;
import com.github.tjake.mvbench.playlist.AbstractPlaylist;
import io.airlift.airline.Command;
import io.airlift.airline.HelpOption;
import io.airlift.airline.Option;
import io.airlift.airline.SingleCommand;

/**
 * Created by jake on 8/14/15.
 */
@Command(name = "mvbench", description = "benchmark for materialized views")
public class Bench
{
    @Inject
    public HelpOption helpOption;

    @Option(name = {"-u", "--num-users"}, description = "The total number of users in the system")
    public int numUsers = 10000;

    @Option(name = {"-p", "--songs-per-playlist"}, description = "The number of songs per playlist")
    public int songsPerPlaylists = 20;

    @Option(name = {"-s", "--num-songs"}, description = "The total number of songs in the system")
    public int numSongs = 100000;

    @Option(name = {"-a", "--num-artists"}, description = "The total number of artists in the system")
    public int numArtists = 1000;

    @Option(name = {"-g", "--num-genres"}, description = "The total number of genres in the system")
    public int numGenres = 10;

    @Option(name = {"-d", "--percent-delete"})
    public double percentDelete = 0.25;

    @Option(name = {"-b", "--percent-update"})
    public double percentUpdate = 0.25;

    @Option(name = {"-n", "--num-iter"}, description = "The number of iterations")
    public long numberIterations = 100000;

    @Option(name = {"--seed"}, description = "The seed")
    public long seed = 1238888L;

    @Option(name = {"--manual"}, description = "Should the views be managed manually")
    public boolean isManual = false;

    @Option(name = {"--endpoint"}, description = "the cassandra endpoint")
    public String cassandraEndpoint = "127.0.0.1";

    @Option(name = {"--maxinflight"}, description = "max requests in flight at a time")
    public int maxInFlight = 1000;

    @Option(name = {"--maxplaylistspersec"}, description = "generate max playlists per sec")
    public int maxPlaylistsPerSec = 5000;

    void run()
    {
        Random random = new Random(seed);

        AbstractPlaylist.Factory factory = new AbstractPlaylist.Factory(random, numUsers, songsPerPlaylists, numSongs, numArtists, numGenres, percentDelete, percentUpdate, isManual);

        PoolingOptions poolingOpts = new PoolingOptions()
                .setConnectionsPerHost(HostDistance.LOCAL, 8, 8)
                .setMaxRequestsPerConnection(HostDistance.LOCAL, 128)
                .setNewConnectionThreshold(HostDistance.LOCAL, 100);

        Cluster cluster = new Cluster.Builder()
                .addContactPoint(cassandraEndpoint)
                .withoutMetrics()
                .withoutJMXReporting()
                .withPoolingOptions(poolingOpts)
                .withRetryPolicy(DowngradingConsistencyRetryPolicy.INSTANCE)
                .build();

        Session session = cluster.connect(isManual ? "manual" : "mview");

        RateLimiter limiter = RateLimiter.create(maxPlaylistsPerSec);

        System.out.println("Writing for "+numberIterations+ " iterations to " + (isManual ? "manual" : "materialied view") + " schema.");

        for (int i = 0; i < numberIterations; i++)
        {

            while (AbstractPlaylist.tracker.get() > maxInFlight)
                Uninterruptibles.sleepUninterruptibly(1, TimeUnit.MILLISECONDS);

            limiter.acquire();

            factory.next().write(session);

            if (i % 10000 == 0)
                System.err.println("Iteration " + i + ", Tracker at " + AbstractPlaylist.tracker.get());
        }

        while (AbstractPlaylist.tracker.get() > 1)
        {
            System.out.println("Waiting for "+AbstractPlaylist.tracker.get());
            Uninterruptibles.sleepUninterruptibly(1, TimeUnit.SECONDS);
        }

        cluster.close();
    }

    public static void main(String[] args)
    {
        Bench bench = SingleCommand.singleCommand(Bench.class).parse(args);

        if (bench.helpOption.showHelpIfRequested())
        {
            return;
        }


        final ConsoleReporter reporter = ConsoleReporter.forRegistry(AbstractPlaylist.registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .outputTo(System.out)
                .build();

        File output = new File("./reports" + (bench.isManual ? "/manual" : "/view"));
        System.out.println("Writing output to: " + output.getAbsolutePath());

        final CsvReporter csv = CsvReporter.forRegistry(AbstractPlaylist.registry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(output);

        reporter.start(30, TimeUnit.SECONDS);
        csv.start(1, TimeUnit.SECONDS);

        try
        {
            bench.run();
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(-1);
        }

        csv.stop();
        reporter.stop();

        System.out.println("===TOTAL REPORT===");

        try
        {
            for (String line : Files.readAllLines(new File(output, "total.csv").toPath(), Charset.defaultCharset()))
                System.out.println(line);

            System.out.flush();
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        System.exit(0);
    }
}
