package rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.util.ImmutableBitSet;

import convention.PConvention;

import java.util.List;
import java.util.ArrayList;

// Count, Min, Max, Sum, Avg
public class PAggregate extends Aggregate implements PRel {

    public PAggregate(
            RelOptCluster cluster, // RelOptCluster -> A cluster of relational expressions
            RelTraitSet traitSet, // RelTraitSet -> A set of relational expression traits
            List<RelHint> hints, // List of hints -> Hints are used to provide additional information to the planner to make better decisions
            RelNode input, // Input relational expression - The relational expression which is the input to this relational expression
            ImmutableBitSet groupSet, // ImmutableBitSet groupSet -> Grouping set - A set of columns that are being grouped
            List<ImmutableBitSet> groupSets, // List of ImmutableBitSet groupSets -> Grouping sets - A list of sets of columns that are being grouped
            List<AggregateCall> aggCalls) { // List of AggregateCall aggCalls -> Aggregate calls - A list of aggregate functions to be computed - To consider: Sum, Avg, Count, Min, Max, Distinct?
        super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof PConvention;
    }

    @Override
    public Aggregate copy(RelTraitSet traitSet, RelNode input, ImmutableBitSet groupSet,
                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new PAggregate(getCluster(), traitSet, hints, input, groupSet, groupSets, aggCalls);
    }

    @Override
    public String toString() {
        return "PAggregate";
    }

    private List<Object[]> rows = new ArrayList<>();
    private List<Object[]> aggregatedRows = new ArrayList<>();


    // returns true if successfully opened, false otherwise
    // Do the aggregation here
    @Override
    public boolean open() {
        // We would need to use the input to get the rows, group the rows according to the list of groupSets, and then apply the aggregate functions according to the list of aggCalls
        logger.trace("Opening PAggregate");
        PRel child = (PRel) this.input;
        if (child.open()) {

            // Get the rows from the input
            while (child.hasNext()) {
                rows.add(child.next());
            }

            for (Object[] row : rows) {
            }
        }


        return false;
    }

    // any postprocessing, if needed
    @Override
    public void close() {
        logger.trace("Closing PAggregate");
        PRel child = (PRel) this.input;
        child.close();
        return;
    }

    // returns true if there is a next row, false otherwise
    @Override
    public boolean hasNext() {
        logger.trace("Checking if PAggregate has next");
        if (aggregatedRows.size() > 0) {
            return true;
        }
        return false;
    }

    // returns the next row
    @Override
    public Object[] next() {
        logger.trace("Getting next row from PAggregate");
        if (hasNext()) {
            return aggregatedRows.remove(0);
        }
        return null;
    }

}