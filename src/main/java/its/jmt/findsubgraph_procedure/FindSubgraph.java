package its.jmt.findsubgraph_procedure;

import java.util.stream.Stream;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.traversal.Evaluation;
import org.neo4j.graphdb.traversal.Evaluator;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Uniqueness;
import org.neo4j.jmx.Description;
import org.neo4j.logging.Log;
import org.neo4j.procedure.Context;
import org.neo4j.procedure.Name;
import org.neo4j.procedure.Procedure;

public class FindSubgraph {
	
	//Only static fields and @Context-annotated fields are allowed in Procedure classes
	
	// This field declares that we need a GraphDatabaseService
    // as context when any procedure in this class is invoked
    @Context
    public GraphDatabaseService db;

    // This gives us a log instance that outputs messages to the
    // standard log, normally found under `data/log/console.log`
    @Context
    public Log log;
    
    /* #1 Demo procedure
     * 
     * This is the first procedure in the class - a procedure that return the subgraph of a given start node.
     * - and it is ok that add another procedure in this class as this one.
     * 
     * The arguments to this procedure are annotated with the {@link Name} annotation and define the position,
     * name and type of arguments required to invoke this procedure.There is a limited set of types you can use
     * for arguments, these are as follows:
     * 
     * {@link String}
     * {@link Long} or {@code long}
     * {@link Double} or {@code double}
     * {@link Number}
     * {@link Boolean} or {@code boolean}
     * {@link java.util.Map} with key {@link String} and value {@link Object}
     * {@link java.util.List} of elements of any valid argument type, including {@link java.util.List}
     * {@link Object}, meaning any of the valid argument types
     * 
     * @param startNode: the node to query by
     * 
     * 
     * @return the subgraph(nodes and rels) of the given node
     * 
     * */
    @Description("demo.findSubgraph(node) | Find out the subgraph of a given startnode, return its subgraph.")
    @Procedure(value="demo.findSubgraph")
    public Stream<PathResult> findSubgraphInDemo(@Name("startNode")Node startNode){
    	TraversalDescription mTraversal =db.traversalDescription()
    			.breadthFirst()
    			.uniqueness(Uniqueness.NODE_PATH)
    			.evaluator(Evaluators.fromDepth(1))
    			//.evaluator(Evaluators.toDepth(3)
    			.evaluator(new CustomNodeFilteringEvaluatorDEMO(startNode));
    	return mTraversal.traverse(startNode).iterator().stream().map(PathResult::new);
    }
    
    
    public class CustomNodeFilteringEvaluatorDEMO implements Evaluator{
    	
    	private final Node startNode;

    	public CustomNodeFilteringEvaluatorDEMO(Node startNode) {
			// TODO Auto-generated constructor stub
    		this.startNode=startNode;
		}

		@Override
		public Evaluation evaluate(Path path) {
			// TODO Auto-generated method stub
			Node currentNode=path.endNode();
			if(currentNode.hasLabel(mLabels.sex)||
					currentNode.hasLabel(mLabels.status)||
					currentNode.hasLabel(mLabels.vehqua)||
					currentNode.hasLabel(mLabels.vehbrd)||
					currentNode.hasLabel(mLabels.vehclr)||
					currentNode.hasLabel(mLabels.vehtp)||
					currentNode.hasLabel(mLabels.vioid)){
				return Evaluation.INCLUDE_AND_PRUNE;
			}
			return Evaluation.INCLUDE_AND_CONTINUE;
		}
    }
    
    
    /* #2 JMT procedure
     * 
     * This is the second procedure in the class - a procedure that return the subgraph of a given start node.
     * - and it is ok that add another procedure in this class as this one.
     * 
     * The arguments to this procedure are annotated with the {@link Name} annotation and define the position,
     * name and type of arguments required to invoke this procedure.There is a limited set of types you can use
     * for arguments, these are as follows:
     * 
     * {@link String}
     * {@link Long} or {@code long}
     * {@link Double} or {@code double}
     * {@link Number}
     * {@link Boolean} or {@code boolean}
     * {@link java.util.Map} with key {@link String} and value {@link Object}
     * {@link java.util.List} of elements of any valid argument type, including {@link java.util.List}
     * {@link Object}, meaning any of the valid argument types
     * 
     * @param startNode: the node to query by
     * 
     * 
     * @return the subgraph(nodes and rels) of the given node
     * 
     * */
    @Description("jmt.findSubgraph(node) | Find out the subgraph of a given startnode, return its subgraph.")
    @Procedure(value="jmt.findSubgraph")
    public Stream<PathResult> findsubgraph(@Name("startNode")Node startNode){
    	TraversalDescription mTraversal =db.traversalDescription()
    			.breadthFirst()
    			.uniqueness(Uniqueness.NODE_PATH)
    			.evaluator(Evaluators.fromDepth(1))
    			//.evaluator(Evaluators.toDepth((int)depth))
    			.evaluator(new CustomNodeFilteringEvaluatorJMT(startNode));
    	return mTraversal.traverse(startNode).iterator().stream().map(PathResult::new);
    }
    
    
    public class CustomNodeFilteringEvaluatorJMT implements Evaluator{
    	
    	private final Node startNode;

    	public CustomNodeFilteringEvaluatorJMT(Node startNode) {
			// TODO Auto-generated constructor stub
    		this.startNode=startNode;
		}
		
		@Override
		public Evaluation evaluate(Path path) {
			// TODO Auto-generated method stub
			Node currentNode=path.endNode();
			if(currentNode.hasLabel(mLabels.驾驶员性别)||
					currentNode.hasLabel(mLabels.准驾车型)||
					currentNode.hasLabel(mLabels.驾驶证状态)||
					currentNode.hasLabel(mLabels.机动车品牌)||
					currentNode.hasLabel(mLabels.机动车类型)||
					currentNode.hasLabel(mLabels.机动车颜色)||
					currentNode.hasLabel(mLabels.违法代码)){
				return Evaluation.INCLUDE_AND_PRUNE;
			}
			return Evaluation.INCLUDE_AND_CONTINUE;
		}	
    }
    
    
    /* #3 JMT procedure
     * 
     * This is the second procedure in the class - a procedure that return the subgraph of a given start node.
     * - and it is ok that add another procedure in this class as this one.
     * 
     * The arguments to this procedure are annotated with the {@link Name} annotation and define the position,
     * name and type of arguments required to invoke this procedure.There is a limited set of types you can use
     * for arguments, these are as follows:
     * 
     * {@link String}
     * {@link Long} or {@code long}
     * {@link Double} or {@code double}
     * {@link Number}
     * {@link Boolean} or {@code boolean}
     * {@link java.util.Map} with key {@link String} and value {@link Object}
     * {@link java.util.List} of elements of any valid argument type, including {@link java.util.List}
     * {@link Object}, meaning any of the valid argument types
     * 
     * @param startNode: the node to query by
     * @param toDepth: the depth of the query, using for test
     * 
     * @return the subgraph(nodes and rels) of the given node
     * 
     * */
    @Description("jmt.findSubgraphWithDepth(node,depth) | Find out the subgraph of a given startnode, return its subgraph.")
    @Procedure(value="jmt.findSubgraphWithDepth")
    public Stream<PathResult> findsubgraphwithdepth(@Name("startNode")Node startNode,
    									   			@Name("toDepth")long depth){
    	TraversalDescription mTraversal =db.traversalDescription()
    			.breadthFirst()
    			.uniqueness(Uniqueness.NODE_PATH)
    			.evaluator(Evaluators.fromDepth(1))
    			.evaluator(Evaluators.toDepth((int)depth))
    			.evaluator(new CustomNodeFilteringEvaluatorJMTWithDepth(startNode));
    	return mTraversal.traverse(startNode).iterator().stream().map(PathResult::new);
    }
    
    
    public class CustomNodeFilteringEvaluatorJMTWithDepth implements Evaluator{
    	
    	private final Node startNode;

    	public CustomNodeFilteringEvaluatorJMTWithDepth(Node startNode) {
			// TODO Auto-generated constructor stub
    		this.startNode=startNode;
		}
		
		@Override
		public Evaluation evaluate(Path path) {
			// TODO Auto-generated method stub
			Node currentNode=path.endNode();
			if(currentNode.hasLabel(mLabels.驾驶员性别)||
					currentNode.hasLabel(mLabels.准驾车型)||
					currentNode.hasLabel(mLabels.驾驶证状态)||
					currentNode.hasLabel(mLabels.机动车品牌)||
					currentNode.hasLabel(mLabels.机动车类型)||
					currentNode.hasLabel(mLabels.机动车颜色)||
					currentNode.hasLabel(mLabels.违法代码)){
				return Evaluation.INCLUDE_AND_PRUNE;
			}
			return Evaluation.INCLUDE_AND_CONTINUE;
		}	
    }
    
    
    /*
     * Define the labels using in JMT
     * */
    public static enum mLabels implements Label{
    	//For demo
    	driver,license,vehicle,violation,viosur,
    	sex,status,vehbrd,vehclr,vehqua,vehtp,vioid,
    	//For JMT
    	驾驶员性别,准驾车型,驾驶证状态,机动车品牌,机动车类型,机动车颜色,违法代码
    }
    
    /*
     * This is the output record for our procedure. All procedures that return results return them as a 
     * Stream of Records, where the records are defined like this one - customized to fit what the 
     * procedure is returning.
     * 
     * These classes can only have public non-final fields, and the fields must be one of the 
     * following types:
     * 
     * {@link String}
     * {@link Long} or {@code long}
     * {@link Double} or {@code double}
     * {@link Number}
     * {@link Boolean} or {@code boolean}
     * {@link org.neo4j.graphdb.Node}
     * {@link org.neo4j.graphdb.Relationship}
     * {@link org.neo4j.graphdb.Path}
     * {@link java.util.Map} with key {@link String} and value {@link Object}
     * {@link java.util.List} of elements of any valid field type, including {@link java.util.List}
     * {@link Object}, meaning any of the valid field types
     * 
     * 
     * 
     * */
    
    public class PathResult{
    	public Path path;
    	
    	PathResult(Path path){
    		this.path=path;
    	}
    }
}
