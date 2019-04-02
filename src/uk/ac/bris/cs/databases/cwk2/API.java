package uk.ac.bris.cs.databases.cwk2;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import uk.ac.bris.cs.databases.api.*;

/**
 *
 * @author csxdb
 */
public class API implements APIProvider {

    private final Connection c;

    public API(Connection c) {
        this.c = c;
    }

    /* A.1 */

    @Override
    public Result<Map<String, String>> getUsers() {

        if (c == null) { throw new IllegalStateException (); }

        HashMap<String, String> users = new HashMap<> ();
        String sql = "SELECT username, name FROM Person";

        try (PreparedStatement ps = c.prepareStatement (sql)) {
            ResultSet rs = ps.executeQuery ();

            while (rs.next ()) {
                String username = rs.getString ("username");
                String name = rs.getString ("name");
                users.put (username, name);
            }

        } catch (SQLException e) {
            throw new RuntimeException (e);
        }
        return Result.success (users);
    }

    @Override
    public Result<PersonView> getPersonView(String username) {

        if (c == null) { throw new IllegalStateException (); }
        String sql = "SELECT * FROM Person WHERE username = ?";
        PersonView user;

        try (PreparedStatement ps = c.prepareStatement (sql)) {
            ps.setString (1, username);
            ResultSet rs = ps.executeQuery ();

            if (rs.next ()) {
                String name = rs.getString ("name");
                String studentId = rs.getString ("stuId");
                user = new PersonView (name, username, studentId);
                return Result.success (user);
            }
            return Result.failure ("Username doesn't match any existing user");

        } catch (SQLException e){
            return Result.fatal (e.toString ());
        }
    }

    @Override
    public Result addNewPerson(String name, String username, String studentId) {
        if (c == null) { throw new IllegalStateException (); }
        if (name == null || username == null) {
            return Result.fatal ("Ooopps, something wen't wrong");
        }

        String sql = "SELECT count(1) as c FROM Person WHERE username = ?";

        try (PreparedStatement ps = c.prepareStatement (sql)) {
            ps.setString (1, username);
            ResultSet rs = ps.executeQuery ();

            if (rs.next ()) {
                if (rs.getInt("c") > 0) {
                    return Result.failure ("This username is already being used");
                }
            }
        } catch (SQLException e) {
           return Result.fatal (e.toString ());
        }
        // User doesn't exist yet: Add User to Database
        String AnotherSql = "INSERT INTO Person (name, username, stuId) " +
                "VALUES (?, ?, ?)";
        try (PreparedStatement ps = c.prepareStatement (AnotherSql)) {
            ps.setString (1 ,name);
            ps.setString (2 ,username);
            ps.setString (3 ,studentId);
            ps.executeQuery ();

            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
                return Result.fatal ("Internal error: user couldn't be created");
            } catch (SQLException f) {
                return Result.fatal (e.toString ());
            }
        }

        return Result.success ();
    }

    /* A.2 */

    @Override
    public Result<List<SimpleForumSummaryView>> getSimpleForums() {
        if (c == null) { throw new IllegalStateException (); }

        ArrayList<SimpleForumSummaryView> forums = new ArrayList<> ();
        String sql = "SELECT * FROM Forum" +
                " ORDER BY title ASC";

        try (PreparedStatement ps = c.prepareStatement (sql)) {
            ResultSet rs = ps.executeQuery ();
            while(rs.next ()) {
                int id = rs.getInt ("id");
                String title = rs.getString ("title");
                forums.add (new SimpleForumSummaryView (id, title));
            }
        }
        catch (SQLException e) {
            throw new RuntimeException (e);
        }
        return Result.success (forums);
    }

    @Override
    public Result createForum(String title) {

        if (c == null) { throw new IllegalStateException (); }
        if (title == null || title.length () == 0) {
            return Result.failure ("Incorrect 'title' format: couldn't create Forum");
        }

        String sql = "SELECT count(1) as c FROM Forum WHERE title = ?";

        try (PreparedStatement ps = c.prepareStatement (sql)) {
            ps.setString (1, title);
            ResultSet rs = ps.executeQuery ();

            if (rs.next ()) {
                if (rs.getInt("c") > 0) {
                    return Result.failure ("This Forum name is already being used");
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException (e);
        }
        // Forum name doesn't exist yet: Add num Forum to Database
        String WriteSql = "INSERT INTO Forum (title) VALUES (?)";
        try (PreparedStatement ps = c.prepareStatement (WriteSql)) {
            ps.setString (1 ,title);
            ps.executeQuery ();

            c.commit();
        } catch (SQLException e) {
            try {
                c.rollback();
                return Result.fatal ("Internal error: forum couldn't be created");
            } catch (SQLException f) {
                throw new RuntimeException (f);
            }
        }
        return Result.success ();
    }

    /* A.3 */

    @Override
    public Result<List<ForumSummaryView>> getForums() {
        if (c == null) { throw new IllegalStateException (); }

        ArrayList<ForumSummaryView> forums = new ArrayList<> ();
        String sql = "SELECT * FROM Topic " +
                    "INNER JOIN Forum " +
                    "ON Forum.id = Topic.forumId ";

        try (PreparedStatement ps = c.prepareStatement (sql)) {
           ResultSet rs = ps.executeQuery ();

           while (rs.next ()) {
               int id = rs.getInt ("Forum.id");
               String title = rs.getString ("Forum.title");
               int topicId = rs.getInt ("Topic.id");
               String topicTitle = rs.getString ("Topic.title");

               SimpleTopicSummaryView lastTopic = new SimpleTopicSummaryView (topicId, id, topicTitle);
               forums.add (new ForumSummaryView (id, title, lastTopic));
           }
        }
        catch (SQLException e) {
            throw new RuntimeException (e);
        }
        return Result.success (forums);
    }


    @Override
    public Result<ForumView> getForum(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<SimpleTopicView> getSimpleTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<PostView> getLatestPost(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createPost(int topicId, String username, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result createTopic(int forumId, String username, String title, String text) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<Integer> countPostsInTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* B.1 */

    @Override
    public Result likeTopic(String username, int topicId, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result likePost(String username, int topicId, int post, boolean like) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<List<PersonView>> getLikers(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<TopicView> getTopic(int topicId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    /* B.2 */

    @Override
    public Result<List<AdvancedForumSummaryView>> getAdvancedForums() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedPersonView> getAdvancedPersonView(String username) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Result<AdvancedForumView> getAdvancedForum(int id) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
