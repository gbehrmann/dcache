/*
 * dCache - http://www.dcache.org/
 *
 * Copyright (C) 2016 Deutsches Elektronen-Synchrotron
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.dcache.webdav;

import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;

/**
 * Handler that redirects all requests to a different port on the local server.
 */
public class RedirectToPortHandler extends AbstractHandler
{
    private int port;

    public int getPort()
    {
        return port;
    }

    public void setPort(int port)
    {
        this.port = port;
    }

    @Override
    public void handle(String target, Request baseRequest, HttpServletRequest request,
                       HttpServletResponse response) throws IOException, ServletException
    {
        response.sendRedirect(response.encodeRedirectURL(request.getScheme() + "://" + request.getServerName() + ":" + port + "/"));
    }
}
