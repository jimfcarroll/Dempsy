package net.dempsy;

import org.slf4j.Logger;

import net.dempsy.config.Cluster;

public class DempsyBaseTest {
    /**
     * Setting 'hardcore' to true causes EVERY SINGLE IMPLEMENTATION COMBINATION to be used in 
     * every runAllCombinations call. This can make tests run for a loooooong time.
     */
    public static boolean hardcore = false;

    protected Logger LOGGER;

    /**
     * Contains a generic node specification. It needs to following variables set:
     * 
     * <ul>
     * <li>routing-strategy - set the to the routing strategy id. </li>
     * <li>container-type - set to the container type id </li>
     * </ul>
     * 
     * <p>It autowires all of the {@link Cluster}'s that appear in the same context.</p>
     * 
     * <p>Currently it directly uses the {@code BasicStatsCollector}</p>
     */
    public static String nodeCtx = "classpath:/td/node.xml";

    protected DempsyBaseTest(final Logger logger) {
        this.LOGGER = logger;
    }

}
