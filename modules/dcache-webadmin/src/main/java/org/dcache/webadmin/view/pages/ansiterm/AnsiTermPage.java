package org.dcache.webadmin.view.pages.ansiterm;

import org.apache.wicket.markup.head.IHeaderResponse;
import org.apache.wicket.markup.head.JavaScriptHeaderItem;
import org.apache.wicket.markup.html.basic.Label;
import org.apache.wicket.markup.html.form.Form;
import org.apache.wicket.protocol.https.RequireHttps;

import java.util.concurrent.TimeUnit;

import org.dcache.util.NetworkUtils;
import org.dcache.webadmin.view.pages.AuthenticatedWebPage;
import org.dcache.webadmin.view.pages.basepage.BasePage;

/**
 * Created by arossi on 5/8/15.
 */
@RequireHttps
public class AnsiTermPage extends BasePage implements AuthenticatedWebPage {
    public AnsiTermPage() {
        add(new Label("url",
                        getProtocol() + getHost() + getPort() + getContext()));
        add(new Label("rows", getRows()));
        add(new Label("cols", getCols()));
    }

    @Override protected void addAutoRefreshToForm(Form<?> form, long refresh,
                    TimeUnit unit) {
        super.addAutoRefreshToForm(form, refresh, unit);
    }

    protected void renderHeadInternal(IHeaderResponse response) {
        super.renderHeadInternal(response);
        response.render(JavaScriptHeaderItem.forUrl("js/term.js"));
        response.render(JavaScriptHeaderItem.forUrl("js/adminterm.js"));
    }

    private String getProtocol() {
        return "wss://";
    }

    private String getHost() {
       return NetworkUtils.getCanonicalHostName();
//        return "localhost";
    }

    private String getPort() {
        return ":" + getWebadminApplication().getHttpsPort();
    }

    private String getContext() {
        return "/adminshell";
    }

    private int getRows() {
        return getWebadminApplication().getTerminalRows();
    }

    private int getCols() {
        return getWebadminApplication().getTerminalCols();
    }
}
