package hello.client.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.yarn.client.YarnClient;

@Controller
@RequestMapping("/launch-yarn-app")
public class ApplicationStartController {

    @Autowired
    private ApplicationContext context;

    @RequestMapping(method= RequestMethod.GET)
    public @ResponseBody String launchYarnApp() {
        context.getBean(YarnClient.class).submitApplication();
        return "Launched";
    }
}
