package source;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;

@Path("generic")
public class Smart {
    @SuppressWarnings("unused")
    @Context
    private UriInfo context;

    /**
     * Default constructor. 
     */
    public Smart() {
        // TODO Auto-generated constructor stub
    }

    /**
     * Retrieves representation of an instance of Smart
     * @return an instance of String
     */
    @GET
    @Produces("application/xml")
    public String getXml() {
        // TODO return proper representation object
        throw new UnsupportedOperationException();
    }

    /**
     * PUT method for updating or creating an instance of Smart
     * @param content representation for the resource
     * @return an HTTP response with content of the updated or created resource.
     */
    @PUT
    @Consumes("application/html")
    public void putXml(String content) {
    }
  
}


