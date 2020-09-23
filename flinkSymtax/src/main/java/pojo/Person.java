package pojo;

/**
 * @author ：kenor
 * @date ：Created in 2020/8/29 18:38
 * @description：
 * @version: 1.0
 */

public class Person {
    private Integer id;
    private String name;
    private String gender;


    public Person(Integer id, String name, String gender) {
        this.id = id;
        this.name = name;
        this.gender = gender;
        System.out.println("2222");
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    @Override
    public String toString() {
        return "Person{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", gender='" + gender + '\'' +
                '}';
    }
}
