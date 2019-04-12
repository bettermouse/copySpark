package org.apache.spark.util;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public  class HelloServlet111 extends HttpServlet {
    private static final long serialVersionUID = 1L;
    private String msg = "Hello World!";

    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        response.setContentType("text/html");
        //response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().println("<h1>" + msg + "</h1>");
        // response.getWriter().println("session=" + request.getSession(true).getId());
    }

    public static void main(String[] args) throws Exception {
        Server server = new Server(8080);
        ContextHandlerCollection collection = new ContextHandlerCollection();
        server.setHandler(collection);

        ServletContextHandler context = new ServletContextHandler();
       // collection.addHandler(context);
        context.addServlet(new ServletHolder(new HelloServlet111()), "/hello");

        //不直接添加 context 了
//        if(!context.isStarted()){
//            context.start();
//        }
        GzipHandler gzipHandler = new GzipHandler();
        gzipHandler.setHandler(context);
        collection.addHandler(gzipHandler);
       // gzipHandler.start()
        // http://localhost:8080/hello
        server.start();
        server.join();
    }
}