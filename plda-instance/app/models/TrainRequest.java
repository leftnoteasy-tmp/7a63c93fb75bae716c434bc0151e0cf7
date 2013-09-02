package models;

import javax.persistence.Entity;
import javax.persistence.Id;

import play.data.validation.Constraints.Required;
import play.db.ebean.Model;

@Entity
public class TrainRequest extends Model {
  @Id
  public long id;
  
  @Required
  public int numTopic;
  
  @Required
  public String path;
}
