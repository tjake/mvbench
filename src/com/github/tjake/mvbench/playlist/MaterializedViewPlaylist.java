package com.github.tjake.mvbench.playlist;


import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

import com.datastax.driver.core.*;

/**
 * Created by jake on 8/15/15.
 */
public class MaterializedViewPlaylist extends AbstractPlaylist
{

    volatile static PreparedStatement addStatement = null;
    volatile static PreparedStatement updateStatement = null;
    volatile static PreparedStatement deleteStatement = null;

    protected MaterializedViewPlaylist(String userName, String playlistName, List<Song> songList, Type type)
    {
        super(userName, playlistName, songList, type);
    }


    @Override
    ResultSetFuture add(Session session)
    {
        if (addStatement == null)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO user_playlists(user_name, playlist_name, song_id, added_time, ")
              .append("artist_name, genre)VALUES(?,?,?,?,?,?)");

            addStatement = session.prepare(sb.toString());
        }

        Long now = System.currentTimeMillis();
        BatchStatement batch = new BatchStatement(BatchStatement.Type.LOGGED);
        batch.setConsistencyLevel(ConsistencyLevel.QUORUM);

        for (Song song : songList)
            batch.add(addStatement.bind(userName, playlistName, song.songId, now, song.artist, song.genre));

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
    ResultSetFuture update(Session session)
    {
        if (updateStatement == null)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("UPDATE user_playlists SET last_played = ? WHERE user_name = ? AND playlist_name = ? AND song_id = ?");

            updateStatement = session.prepare(sb.toString());
        }

        BoundStatement statement = updateStatement.bind(System.currentTimeMillis(), userName, playlistName, songList.get(1).songId);
        statement.setConsistencyLevel(ConsistencyLevel.QUORUM);


        tracker.incrementAndGet();
        final long startTime = System.nanoTime();

        ResultSetFuture future = session.executeAsync(statement);


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

        return future;
    }

    @Override
    ResultSetFuture delete(Session session)
    {
        if (deleteStatement == null)
        {
            StringBuilder sb = new StringBuilder();
            sb.append("DELETE FROM user_playlists WHERE user_name = ? AND playlist_name = ?");

            deleteStatement = session.prepare(sb.toString());
        }

        BoundStatement statement = deleteStatement.bind(userName, playlistName);
        statement.setConsistencyLevel(ConsistencyLevel.QUORUM);

        tracker.incrementAndGet();
        final long startTime = System.nanoTime();

        ResultSetFuture future = session.executeAsync(statement);

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

        return future;
    }
}
