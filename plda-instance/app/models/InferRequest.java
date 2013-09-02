package models;

import javax.persistence.Entity;
import javax.persistence.Id;

import play.db.ebean.Model;

@Entity
public class InferRequest extends Model {
  @Id
  public long id;
  
  public String document;
}
