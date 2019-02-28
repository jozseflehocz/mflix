package mflix.api.daos;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import com.mongodb.client.result.UpdateResult;
import mflix.api.models.Session;
import mflix.api.models.User;
import org.bson.Document;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

import static com.mongodb.client.model.Updates.set;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;

@Configuration
public class UserDao extends AbstractMFlixDao {

    private static final String EMAILFIELD="email";

    private final MongoCollection<User> usersCollection;
    //Ticket: User Management - do the necessary changes so that the sessions collection
    //returns a Session object
    private final MongoCollection<Session> sessionsCollection;
    private final Logger log;

    @Autowired
    public UserDao(
            MongoClient mongoClient, @Value("${spring.mongodb.database}") String databaseName) {
        super(mongoClient, databaseName);
        CodecRegistry pojoCodecRegistry =
                fromRegistries(
                        MongoClientSettings.getDefaultCodecRegistry(),
                        fromProviders(PojoCodecProvider.builder().automatic(true).build()));

        usersCollection = db.getCollection("users", User.class).withCodecRegistry(pojoCodecRegistry);
        log = LoggerFactory.getLogger(this.getClass());
        //Ticket: User Management - implement the necessary changes so that the sessions
        // collection returns a Session objects instead of Document objects.
        sessionsCollection = db.getCollection("sessions", Session.class).withCodecRegistry(pojoCodecRegistry);
    }

    /**
     * Inserts the `user` object in the `users` collection.
     *
     * @param user - User object to be added
     * @return True if successful, throw IncorrectDaoOperation otherwise
     */
    public boolean addUser(User user){
        //Ticket: Durable Writes -  you might want to use a more durable write concern here!

        try {
            usersCollection.insertOne(user);
            return true;
            //Ticket: Handling Errors - make sure to only add new users
            // and not users that already exist.
        } catch (Exception e) {
            throw new IncorrectDaoOperation("User already exists with name " + user.getEmail());
        }
    }

    /**
     * Creates session using userId and jwt token.
     *
     * @param userId - user string identifier
     * @param jwt    - jwt string token
     * @return true if successful
     */
    public boolean createUserSession(String userId, String jwt) {
        //Ticket: User Management - implement the method that allows session information to be
        // stored in it's designated collection.
        try {
            Session session = new Session();
            session.setUserId(userId);
            session.setJwt(jwt);
            sessionsCollection.insertOne(session);
            return true;
            //Ticket: Handling Errors - implement a safeguard against
            // creating a session with the same jwt token.
        } catch (Exception e) {
            throw new IncorrectDaoOperation("Session already exists with userID, jwt " + userId + ", " + jwt);
        }
    }

    /**
     * Returns the User object matching the an email string value.
     *
     * @param email - email string to be matched.
     * @return User object or null.
     */
    public User getUser(String email) {

        try {
            //User user =usersCollection.find(Filters.)
            User user;
            //Ticket: User Management - implement the query that returns the first User object.
            user = usersCollection.find(Filters.eq(EMAILFIELD, email)).first();
            return user;
        } catch(Exception e){
            throw new IncorrectDaoOperation("email: "+email);
        }
    }

    /**
     * Given the userId, returns a Session object.
     *
     * @param userId - user string identifier.
     * @return Session object or null.
     */
    public Session getUserSession(String userId) {
        //Ticket: User Management - implement the method that returns Sessions for a given
        // userId
        try {
            Session session;
            Bson queryFilter = new Document("user_id", userId);
            session = sessionsCollection.find(queryFilter).iterator().tryNext();
            return session;
        }catch(Exception e){
            throw new IncorrectDaoOperation("userId: "+userId);
        }
    }

    public boolean deleteUserSessions(String userId) {
        //Ticket: User Management - implement the delete user sessions method
        try {
            sessionsCollection.deleteMany(Filters.eq("user_id", userId));
            return true;
        } catch (Exception e) {
            throw new IncorrectDaoOperation("userId: " + userId);
        }
    }

    /**
     * Removes the user document that match the provided email.
     *
     * @param email - of the user to be deleted.
     * @return true if user successfully removed
     */
    public boolean deleteUser(String email) {
        // remove user sessions
        //Ticket: User Management - implement the delete user method
        try {
            deleteUserSessions(email);
            usersCollection.deleteMany(Filters.eq(EMAILFIELD, email));

            //Ticket: Handling Errors - make this method more robust by
            // handling potential exceptions.
            return true;
        } catch (Exception e) {
            throw new IncorrectDaoOperation("User does not exists with email " + email);
        }
    }

    /**
     * Updates the preferences of an user identified by `email` parameter.
     *
     * @param email           - user to be updated email
     * @param userPreferences - set of preferences that should be stored and replace the existing
     *                        ones. Cannot be set to null value
     * @return User object that just been updated.
     */
    public boolean updateUserPreferences(String email, Map<String, ?> userPreferences) {

        // make sure to check if userPreferences are not null. If null, return false immediately.
        if (userPreferences == null) {
            throw new IncorrectDaoOperation(
                    "userPreferences cannot be set to null");
        }
        // create query filter and update object.
        Bson updateFilter = new Document(EMAILFIELD, email);
        Bson updateObject = Updates.set("preferences", userPreferences);
        // update one document matching email.
        try {
            UpdateResult res = usersCollection.updateOne(updateFilter, updateObject);
            if (res.getModifiedCount() < 1) {
                log.warn("User `{}` was not updated. Trying to re-write the same `preferences` field: `{}`",
                        email, userPreferences);
            }
            return true;
        } catch (Exception e) {
            throw new IncorrectDaoOperation("updatepreferences error: " + email);
        }
    }
}