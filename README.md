# Dapper.Contrib.Bulk
This extension to Insert, Delete, and Update multiple entities faster, with [Dapper.Contrib](https://github.com/DapperLib/Dapper.Contrib/)

[!["Buy Me A Coffee"](https://www.buymeacoffee.com/assets/img/custom_images/orange_img.png)](https://www.buymeacoffee.com/adamnguyen)

## Nuget
```
dotnet add package Bulk.Dapper
```

## Example
for example  User
```cs
   public class User
    {
        [Key]
        public int id { get; set; }
        public string name { get; set; }
        public int age { get; set; }
    }

    List<User> users = new List<User>
    {
            new User { name = "Joe", age = 10 },
            new User { name = "Donal", age = 10 }
    };
```

## Insert

Normal
```cs
       connection.BulkInsert(users);
```
Async 
```cs
       connection.BulkInsertAsync(users);
```
## Update
Normal
```cs
       connection.BulkUpdate(users);
```
Async 
```cs
       connection.BulkUpdateAsync(users);
```
## Delete
```cs
       connection.BulkDelete(users);
```
Async 
```cs
       connection.BulkDeleteAsync(users);
```
## Set Table name and column name
Can change the table name by attribute Table, column name by attribute ColumnName
```cs
    [Table("Users")]
    public class UserChangeColumn
    {
        [Key]
        public int id { get; set; }

        [ColumnName("name")]
        public string fullname { get; set; }
        public int age { get; set; }
    }
```
## Performance
You can see the performance when I test. The performance is x10 or x100 faster than the old method

![image](https://github.com/nghiand1010/Dapper.Contrib.Bulk/assets/10672343/2c27f0ff-5198-40da-b77e-cc18f3f99759)



## Note
Other attributes are the same as [Dapper.Contrib](https://github.com/DapperLib/Dapper.Contrib/)
