package com.dataartisans.domain;

import java.io.Serializable;
import java.util.LinkedList;
import java.util.List;

public class Customer implements Serializable{

  private String name;

  public Customer(){
    name = "";
  }

  public Customer(String name){
    this.name = name.toLowerCase();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;

  }

  public static List<Customer> getAllCustomers(){
    List<Customer> customers = new LinkedList<>();
    customers.add(new Customer("google"));
    customers.add(new Customer("facebook"));
    customers.add(new Customer("twitter"));
    customers.add(new Customer("apple"));
    customers.add(new Customer("amazon"));
    return customers;
  }

  @Override
  public String toString() {
    return String.format("Customer(%s)", name);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Customer customer = (Customer) o;
    return name.toLowerCase().equals(customer.name.toLowerCase());
  }

  @Override
  public int hashCode() {
    return name.hashCode();
  }
}
