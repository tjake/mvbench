package com.github.tjake.mvbench.playlist;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.util.concurrent.MoreExecutors;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;

/**
 * Created by jake on 8/14/15.
 */
public abstract class AbstractPlaylist
{
    public static final MetricRegistry registry = new MetricRegistry();

    public static Timer totalTimer = registry.register("total", new Timer());
    public static Timer readTimer = registry.register("read", new Timer());
    public static Timer addTimer = registry.register("add", new Timer());
    public static Timer updateTimer = registry.register("update", new Timer());
    public static Timer deleteTimer = registry.register("delete", new Timer());
    public static Timer errorTimer = registry.register("error", new Timer());


    public static AtomicLong tracker = new AtomicLong(0);
    protected static ExecutorService executor = MoreExecutors.sameThreadExecutor();


    public final List<Song> songList;
    public final String userName;
    public final String playlistName;
    public final Type type;

    enum Type {
        ADD, UPDATE, DELETE;

        //Track how many iterations of each operation type
        private long iteration = 0;


        public long increment()
        {
            iteration++;

            //Make sure DELETE and UPDATE don't
            //Trample eachother
            if (this == UPDATE)
                DELETE.iteration = iteration;
            else if (this == DELETE)
                UPDATE.iteration = iteration;

            return iteration;
        }
    }

    protected AbstractPlaylist(String userName, String playlistName, List<Song> songList, Type type)
    {
        this.userName = userName;
        this.playlistName = playlistName;
        this.songList = songList;
        this.type = type;
    }

    public ResultSetFuture write(Session session)
    {
        switch (type)
        {
            case ADD:
                return add(session);
            case UPDATE:
                return update(session);
            case DELETE:
                return delete(session);
            default:
                throw new IllegalStateException();
        }
    }


    abstract ResultSetFuture add(Session session);

    abstract ResultSetFuture update(Session session);

    abstract ResultSetFuture delete(Session session);

    public static class Song
    {
        final String songId;
        final String artist;
        final String genre;

        Song(String songId, String artist, String genre)
        {
            this.songId = songId;
            this.artist = artist;
            this.genre = genre;
        }
    }

    public static class Factory
    {
        private final int numUsers;
        private final int songsPerPlaylist;
        private final int numSongs;
        private final int numArtists;
        private final int numGenres;
        private final double percentDelete;
        private final double percentUpdate;
        private final Random random;
        private final boolean isManual;


        public Factory( Random random, int numUsers, int songsPerPlaylist, int numSongs,
                        int numArtists, int numGenres, double percentDelete, double percentUpdate, boolean isManual)
        {
            //Just avoid having more deletes than adds
            assert percentDelete >= 0.0 && (percentDelete + percentUpdate) < 0.5;
            assert percentUpdate >= 0.0;

            assert numSongs > numGenres;

            this.random = random;
            this.numUsers = numUsers;
            this.songsPerPlaylist = songsPerPlaylist;
            this.numSongs = numSongs;
            this.numArtists = numArtists;
            this.numGenres = numGenres;
            this.percentDelete = percentDelete;
            this.percentUpdate = percentUpdate;
            this.isManual = isManual;
        }

        public AbstractPlaylist next()
        {
            //Make sure we have written at least 1k entries so we don't have more deletes than adds
            double chance = random.nextDouble();
            Type type = Type.ADD;

            if (Type.ADD.iteration > 1000)
            {
                if (chance < percentDelete)
                    type = Type.DELETE;
                else if (chance < percentDelete + percentUpdate)
                    type = Type.UPDATE;
            }

            type.increment();

            assert Type.ADD.iteration > Type.DELETE.iteration;
            assert Type.ADD.iteration > Type.UPDATE.iteration;

            String userName = String.format("user_%d", type.iteration % numUsers);
            String playlistName = String.format("playlist_%d", type.iteration);
            List<Song> songList = new ArrayList<Song>(songsPerPlaylist);


            for (int i = 0; i < songsPerPlaylist; i++, type.iteration++)
            {
                String songId = String.format("song_%d", type.iteration % this.numSongs);
                String artist = String.format("artist_%d", type.iteration % numArtists);
                String genre = String.format("genre_%d", (type.iteration % numArtists) % numGenres);

                songList.add(new Song(songId, artist, genre));
            }

            if (isManual)
                return new ManualPlaylist(userName, playlistName, songList, type);
            else
                return new MaterializedViewPlaylist(userName, playlistName, songList, type);
        }
    }
}
