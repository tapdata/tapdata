package com.tapdata.tm.config.component;

import com.tapdata.tm.Settings.service.SettingsService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Objects;

@Component
public class ProductComponent {
	private final static Logger logger = LogManager.getLogger(ProductComponent.class);

  @Autowired
  private SettingsService settingsService;

  private boolean isCloud;


  @PostConstruct
  public void init() {

    Object buildProfile = settingsService.getByCategoryAndKey("System", "buildProfile");
    if (Objects.isNull(buildProfile)) {
      buildProfile = "DAAS";
    }
    this.isCloud = buildProfile.equals("CLOUD") || buildProfile.equals("DRS") || buildProfile.equals("DFS");

		logger.info("Current product type settings is {}.", buildProfile);
  }

  public boolean isCloud() {
    return isCloud;
  }

  public boolean isDAAS() {
    return !isCloud;
  }
}
