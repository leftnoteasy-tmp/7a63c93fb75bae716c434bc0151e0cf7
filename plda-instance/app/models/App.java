package models;

import javax.persistence.Entity;
import javax.persistence.Id;

import play.data.validation.Constraints;
import play.db.ebean.Model;

@Entity 
public class App extends Model {
  @Id
  public Long id;
  
  @Constraints.Required
  public String name;
  
  @Constraints.Required
  public String type;
  
  public String comment;
  
  public String url;
  public AppState state;
  
  public App() {
    super();
    this.name = "No Name";
    this.url = "";
    this.state = AppState.Init;
    this.type = "No Type";
  }
}
