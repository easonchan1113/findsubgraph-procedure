package its.jmt.findsubgraph_procedure;

import java.util.Map;
import static java.util.Collections.singletonMap;
import static junit.framework.TestCase.assertEquals;
import static java.util.Arrays.asList;

import org.junit.Rule;
import org.junit.Test;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.test.server.HTTP;

public class FindSubgraphTest {

	private static final String MODEL_STATEMENT = 
			//driver #1 and subgraph #1
			"create (n1:driver{name:'d1'})"
			+ "create (n2:license{name:'d1'})"
			+ "create (n3:vehicle{plate:'yueA'})"
			+ "create (n4:violation{vid:'1234'})"
			+ "create (n5:violation{vid:'1234'})"
			+ "create (n6:sex{sex:'m'})"
			+ "create (n7:status{status:'1'})"
			+ "create (n8:vehqua{veh:'c1'})"
			+ "create (n9:vehclr{color:'black'})"
			+ "create (n10:vehbrd{brand:'audi'})"
			+ "create (n11:vehtp{type:'M1'})"
			+ "create (n12:vioid{vid:'1234'})"
			+ "create (n1)-[:SEX]->(n6)"
			+ "create (n1)-[:HASLCS]->(n2)"
			+ "create (n1)-[:HASVEH]->(n3)"
			+ "create (n2)-[:STATUS]->(n7)"
			+ "create (n2)-[:VEHQUA]->(n8)"
			+ "create (n3)-[:COLOR]->(n9)"
			+ "create (n3)-[:BRAND]->(n10)"
			+ "create (n3)-[:TYPE]->(n11)"
			+ "create (n3)-[:VIO]->(n4)"
			+ "create (n3)-[:VIO]->(n5)"
			+ "create (n4)-[:VIOID]->(n12)"
			+ "create (n5)-[:VIOID]->(n12)";
	
	//This rule starts a Neo4j instance
	@Rule
	public Neo4jRule neo4j=new Neo4jRule()
			//With this cypher statement to create a simple test graph
			.withFixture(MODEL_STATEMENT)
			//This is the procedure we want to test
			.withProcedure(FindSubgraph.class);

	@Test
	public void testFindSubgraph() throws Exception{
		HTTP.Response response=HTTP.POST(neo4j.httpURI().resolve("/db/data/transaction/commit").toString(), QUERY);
		/*System.out.println("Output: \n");
		//For test json
		//String results = response.get("results").get(0).get("data").get(0).get("meta").toString();
		String results = response.get("results").get(0).get("data").toString();
		System.out.println(results);*/
		int results=response.get("results").get(0).get("data").size();
		System.out.println("Result: "+results);
		assertEquals(12,results);
	}
	
	private static final Map QUERY = singletonMap("statements",asList(singletonMap("statement", 
			"MATCH (n:driver{name:'d1'}) CALL demo.findSubgraph(n) yield path return path")));
}
