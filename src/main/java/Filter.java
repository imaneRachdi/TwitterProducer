import com.google.common.collect.Lists;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.endpoint.StreamingEndpoint;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;

import java.io.Serializable;

/**
 * Created by imen on 23/12/2016.
 */
class Filterimplements  implements Serializable, TwitterSource.EndpointInitializer {


    public StreamingEndpoint createEndpoint() {
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.locations(Lists.newArrayList(new Location(new Location.Coordinate(-73.935242,40.730610),new Location.Coordinate(-74.935242,41.730610) )));
        endpoint.locations(Lists.newArrayList( new Location(new Location.Coordinate(2.294694,48.858093),new Location.Coordinate(2.19,49) )));
        endpoint.locations(Lists.newArrayList(  new Location(new Location.Coordinate(-0.076132,51.508530),new Location.Coordinate(0.75,51.30) )));
        return endpoint ;
    }


}
