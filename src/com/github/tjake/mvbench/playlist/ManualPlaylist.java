package com.github.tjake.mvbench.playlist;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.datastax.driver.core.*;

/**
 * Created by jake on 8/15/15.
 */
public class ManualPlaylist extends AbstractPlaylist
{
    volatile static PreparedStatement addplaylist = null;
    volatile static PreparedStatement addsong2user = null;
    volatile static PreparedStatement addartist2user = null;
    volatile static PreparedStatement addgenre2user = null;
    volatile static PreparedStatement addrecentlyplayed = null;

    volatile static PreparedStatement updatelastplayed = null;

    volatile static PreparedStatement getplaylist = null;
    volatile static PreparedStatement getlastplated = null;

    volatile static PreparedStatement deleterecentlyplayed = null;
    volatile static PreparedStatement deleteplaylist = null;
    volatile static PreparedStatement deletesong2user = null;
    volatile static PreparedStatement deleteartist2user = null;
    volatile static PreparedStatement deletegenre2user = null;

    protected ManualPlaylist(String userName, String playlistName, List<Song> songList, Type type)
    {
        super(userName, playlistName, songList, type);
    }

    @Override
    ResultSetFuture add(Session session)
    {
        if (addplaylist == null)
        {

            addsong2user = session.prepare("INSERT INTO song_to_user(song_id, user_name, playlist_name," +
                    " added_time)VALUES(?,?,?,?)");


            addartist2user = session.prepare("INSERT INTO artist_to_user(artist_name, user_name, playlist_name," +
                    " song_id)VALUES(?,?,?,?)");

            addgenre2user = session.prepare("INSERT INTO genre_to_user(genre, user_name, playlist_name," +
                    " song_id)VALUES(?,?,?,?)");

            addplaylist = session.prepare("INSERT INTO user_playlists(user_name, playlist_name, song_id, added_time," +
                    " artist_name, genre)VALUES(?,?,?,?,?,?)");
        }


        Long now = System.currentTimeMillis();
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.setConsistencyLevel(ConsistencyLevel.QUORUM);

        for (Song song : songList)
        {
            batch.add(addplaylist.bind(userName, playlistName, song.songId, now, song.artist, song.genre));
            batch.add(addsong2user.bind(song.songId, userName, playlistName, now));
            batch.add(addartist2user.bind(song.artist, userName, playlistName, song.songId));
            batch.add(addgenre2user.bind(song.genre, userName, playlistName, song.songId));
        }

        tracker.incrementAndGet();
        final long startTime = System.nanoTime();

        ResultSetFuture future = session.executeAsync(batch);

        Futures.addCallback(future, new FutureCallback<ResultSet>()
        {
            @Override
            public void onSuccess(ResultSet result)
            {
                long endTime = System.nanoTime();

                totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                addTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                tracker.decrementAndGet();
            }

            @Override
            public void onFailure(Throwable t)
            {
                long endTime = System.nanoTime();
                //totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                errorTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                tracker.decrementAndGet();
            }
        }, executor);

        return future;
    }

    @Override
    ResultSetFuture update(final Session session)
    {
        if (updatelastplayed == null)
        {
            getlastplated = session.prepare("SELECT last_played from user_playlists where user_name = ? " +
                    "and playlist_name = ? and song_id = ?");

            //can also be prepared from delete
            if (deleteplaylist == null)
                deleterecentlyplayed = session.prepare("DELETE FROM recently_played WHERE last_played = ? AND user_name = ?" +
                      " AND playlist_name = ? and song_id = ?");

            addrecentlyplayed = session.prepare("INSERT INTO recently_played(last_played, user_name, playlist_name," +
                    " song_id)VALUES(?,?,?,?)");

            updatelastplayed = session.prepare("UPDATE user_playlists SET last_played = ? WHERE user_name = ? " +
                    "AND playlist_name = ? AND song_id = ?");
        }


        //We need to get the previous value to cleanup the last_played view
        final String songId = songList.get(1).songId;
        BoundStatement boundStatement = getlastplated.bind(userName, playlistName, songId);
        boundStatement.setConsistencyLevel(ConsistencyLevel.QUORUM);

        final BatchStatement updateBatch = new BatchStatement(BatchStatement.Type.LOGGED);
        updateBatch.setConsistencyLevel(ConsistencyLevel.QUORUM);

        tracker.incrementAndGet();

        final long startTime = System.nanoTime();
        ResultSetFuture selectFuture = session.executeAsync(boundStatement);

        Futures.addCallback(selectFuture, new FutureCallback<ResultSet>()
        {
            @Override
            public void onSuccess(ResultSet result)
            {
                long endTime = System.nanoTime();
                readTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);

                Row row = result.one();
                if (row != null)
                {
                    long oldLastPlayed = row.getLong(0);

                    updateBatch.add(deleterecentlyplayed.bind(oldLastPlayed, userName, playlistName, songId));
                }


                long recently_played = System.currentTimeMillis();

                updateBatch.add(updatelastplayed.bind(recently_played, userName, playlistName, songId));
                updateBatch.add(addrecentlyplayed.bind(recently_played, userName, playlistName, songId));


                ResultSetFuture future = session.executeAsync(updateBatch);

                Futures.addCallback(future, new FutureCallback<ResultSet>()
                {
                    @Override
                    public void onSuccess(ResultSet result)
                    {
                        long endTime = System.nanoTime();
                        totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        updateTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        tracker.decrementAndGet();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        long endTime = System.nanoTime();
                        //totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        errorTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        tracker.decrementAndGet();
                    }
                }, executor);
            }

            @Override
            public void onFailure(Throwable t)
            {
                long endTime = System.nanoTime();
                //totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                errorTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                tracker.decrementAndGet();
            }
        }, executor);


       return null;
    }

    @Override
    ResultSetFuture delete(final Session session)
    {

        if (deleteplaylist == null)
        {
            //can also be called from update
            if (deleterecentlyplayed == null)
                deleterecentlyplayed = session.prepare("DELETE FROM recently_played WHERE last_played = ? AND user_name = ?" +
                        " AND playlist_name = ? and song_id = ?");

            getplaylist = session.prepare("SELECT * from user_playlists where user_name = ? AND playlist_name = ?");

            deletesong2user = session.prepare("DELETE FROM song_to_user where song_id = ? AND user_name = ? AND playlist_name = ?");

            deleteartist2user = session.prepare("DELETE FROM artist_to_user where artist_name = ? AND user_name = ? AND playlist_name = ? AND song_id = ?");

            deletegenre2user = session.prepare("DELETE FROM genre_to_user where genre = ? AND user_name = ? AND playlist_name = ? AND song_id = ?");

            deleteplaylist = session.prepare("DELETE from user_playlists where user_name = ? AND playlist_name = ?");
        }


        final BatchStatement batchDelete = new BatchStatement(BatchStatement.Type.LOGGED);
        batchDelete.setConsistencyLevel(ConsistencyLevel.QUORUM);

        tracker.incrementAndGet();
        final long startTime = System.nanoTime();

        //Even though we have the song list we are going to pretend we don't to make this
        //test realistic
        final ResultSetFuture playlistFuture = session.executeAsync(getplaylist.bind(userName, playlistName));

        Futures.addCallback(playlistFuture, new FutureCallback<ResultSet>()
        {
            @Override
            public void onSuccess(ResultSet result)
            {
                long endTime = System.nanoTime();
                readTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);

                for (Row row : result.all())
                {
                    String songId = row.getString("song_id");
                    batchDelete.add(deletesong2user.bind(songId, userName, playlistName));
                    batchDelete.add(deleteartist2user.bind(row.getString("artist_name"), userName, playlistName, songId));
                    batchDelete.add(deletegenre2user.bind(row.getString("genre"), userName, playlistName, songId));

                    if (!row.isNull("last_played"))
                    {
                        long last_played = row.getLong("last_played");
                        batchDelete.add(deleterecentlyplayed.bind(last_played, userName, playlistName, songId));
                    }
                }


                batchDelete.add(deleteplaylist.bind(userName, playlistName));

                ResultSetFuture future = session.executeAsync(batchDelete);

                Futures.addCallback(future, new FutureCallback<ResultSet>()
                {
                    @Override
                    public void onSuccess(ResultSet result)
                    {
                        long endTime = System.nanoTime();
                        totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        deleteTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        tracker.decrementAndGet();
                    }

                    @Override
                    public void onFailure(Throwable t)
                    {
                        long endTime = System.nanoTime();
                        //totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        errorTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                        tracker.decrementAndGet();
                    }
                }, executor);
            }

            @Override
            public void onFailure(Throwable t)
            {
                long endTime = System.nanoTime();
                //totalTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                errorTimer.update(endTime - startTime, TimeUnit.NANOSECONDS);
                tracker.decrementAndGet();
            }
        }, executor);

        return null;
    }
}
