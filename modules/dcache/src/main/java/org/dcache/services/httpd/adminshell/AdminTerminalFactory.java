package org.dcache.services.httpd.adminshell;

import org.springframework.beans.factory.annotation.Required;

import java.io.File;

import diskCacheV111.admin.UserAdminShell;
import dmg.cells.nucleus.CellEndpoint;
import dmg.cells.nucleus.CellMessageSender;
import org.dcache.cells.CellStub;
import org.dcache.util.list.ListDirectoryHandler;

/**
 * Created by arossi on 5/14/15.
 */
public class AdminTerminalFactory implements CellMessageSender {

    private CellEndpoint endpoint;
    private File historyFile;
    private boolean useColor;
    private CellStub pnfsManager;
    private CellStub poolManager;
    private CellStub acm;
    private String prompt;
    private ListDirectoryHandler list;
    private int height;
    private int width;

    @Required
    public void setHeight(int height) {
        this.height = height;
    }

    @Required
    public void setWidth(int width) {
        this.width = width;
    }

    @Required
    public void setHistoryFile(File historyFile) {
        this.historyFile = historyFile;
    }

    @Required
    public void setUseColor(boolean useColor) {
        this.useColor = useColor;
    }

    @Required
    public void setPnfsManager(CellStub stub) {
        this.pnfsManager = stub;
    }

    @Required
    public void setPoolManager(CellStub stub) {
        this.poolManager = stub;
    }

    @Required
    public void setAcm(CellStub stub) {
        this.acm = stub;
    }

    @Required
    public void setPrompt(String prompt) {
        this.prompt = prompt;
    }

    @Required
    public void setListHandler(ListDirectoryHandler list) {
        this.list = list;
    }

    @Override
    public void setCellEndpoint(CellEndpoint endpoint) {
        this.endpoint = endpoint;
    }

    public Terminal createTerminal() {
        return new AnsiTerminal(historyFile, useColor, createShell()) {
            @Override
            public int getWidth() {
                return width;
            }

            @Override
            public int getHeight() {
                return height;
            }
        };
    }

    private UserAdminShell createShell() {
        UserAdminShell shell = new UserAdminShell(prompt);
        shell.setCellEndpoint(endpoint);
        shell.setPnfsManager(pnfsManager);
        shell.setPoolManager(poolManager);
        shell.setAcm(acm);
        shell.setListHandler(list);
        shell.setUser("admin");
        return shell;
    }

}