//Employee Mapper (Distributed Cache)

//Employee Reducer

//Employee Driver(using ToolRunner)

Input Files::
employee.txt:
1,joe,75,000,developer
2,jack,76,000,developer
3,jim,89,000,manager
4,jill,99,000,director
5,chris,88,000,developer
6,ryan,92,000,manager
7,tom,77,000,admin
8,tim,88,000 developer
9,john,56,000,developer
10,james,78,000,developer

employee_reference.txt:
1,joe
4,jill
5,chris
6,ryan
8,tim
7,tom
9,john
10,james

Execution:
hadoop jar /home/cloudera/Desktop/EmployeeSalary.jar EmployeeDriver employee_reference.txt employee.txt developersal
