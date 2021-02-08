package com.knoldus;

public class UserObject {
    private int id;
    private String name;
    private int age;
    private String course;

    public UserObject(){

    }

    public UserObject(int id, String name, int age, String course) {
        this.id = id;
        this.name = name;
        this.age = age;
        this.course = course;
    }

    @Override
    public String toString() {
        return "{Id: " + id + ", Name: " + name + ", Age: " + age + ", Course: " + course + "}";
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {

        this.id = id;
    }

    public String getName() {

        return name;
    }

    public void setName(String Name) {

        this.name = Name;
    }

    public int getAge() {

        return age;
    }

    public void setAge(int Age) {

        this.age = Age;
    }

    public String getCourse() {

        return course;
    }

    public void setCourse(String Course) {

        this.course = Course;
    }
}