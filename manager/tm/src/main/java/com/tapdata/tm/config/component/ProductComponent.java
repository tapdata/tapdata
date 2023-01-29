package com.tapdata.tm.config.component;

import com.tapdata.tm.Settings.service.SettingsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Objects;

@Component
public class ProductComponent {

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
  }

  public boolean isCloud() {
    return isCloud;
  }

  public boolean isDAAS() {
    return !isCloud;
  }
}
