package dlpsearch;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.reindex.ReindexPlugin;
import org.elasticsearch.percolator.PercolatorPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.mustache.MustachePlugin;
import org.elasticsearch.transport.Netty3Plugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * A builder to create an instance of {@link TransportClient} This class
 * pre-installs the {@link Netty3Plugin}, for the client. These plugins are all
 * elasticsearch core modules required.
 */
@SuppressWarnings({ "unchecked", "varargs" })
public class ESTransportClient extends TransportClient {

  private static final Collection<Class<? extends Plugin>> PRE_INSTALLED_PLUGINS = Collections
      .unmodifiableList(Arrays.asList(ReindexPlugin.class, PercolatorPlugin.class, MustachePlugin.class, Netty3Plugin.class));

  @SafeVarargs
  public ESTransportClient(Settings settings, Class<? extends Plugin>... plugins) {
    this(settings, Arrays.asList(plugins));
  }

  public ESTransportClient(Settings settings, Collection<Class<? extends Plugin>> plugins) {
    super(settings, Settings.EMPTY, addPlugins(plugins, PRE_INSTALLED_PLUGINS), null);

  }

  @Override
  public void close() {
    super.close();
  }

}
