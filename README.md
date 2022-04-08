# BigData_HW

## Intro
This repository contains the home-work assignments for Big-Data course.
Each folder contains a solution for a different assignment.

## hw1 - MongoDB & Redis
Develop a system that allows companies to post jobs and candidates can apply for these jobs.
The system will comprise two components: persistent storage (MongoDB) and an in-memory cache (Redis).

### setup with docker-compose:
navigate to ```hw1``` folder and execute:

```bash
docker-compose up 
```

It will start 2 services:
- **MongoDB** - on port ```27017```
- **Redis** - on port ```6379```

### Testing
```pytest -v``` - all tests
```pytest -v -m "not slow"``` - all tests except slow ones