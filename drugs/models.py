import datetime
from django.db import models
from django.utils import timezone

class BaseDrugs(models.Model):
    drug_name = models.CharField(max_length=255)
    drug_effects = models.TextField()
    drug_value = models.IntegerField()
    drug_addiction_level = models.DecimalField(max_digits=5, decimal_places=2)
    drug_added_date = models.DateTimeField(auto_now_add=True)
    drug_updated_date = models.DateTimeField(auto_now=True)
    def __str__(self):
        return self.drug_name
    def added_recently(self):
        return self.drug_added_date >= timezone.now() - datetime.timedelta(days=1)
    def update_recently(self):
        return self.drug_updated_date >= timezone.now() - datetime.timedelta(days=1)
    
class Ingredients(models.Model):
    ingredient_name = models.CharField(max_length=255)
    ingredient_effects = models.TextField()
    ingredient_cost = models.IntegerField()
    ingredient_added_date = models.DateTimeField(auto_now_add=True)
    ingredient_updated_date = models.DateTimeField(auto_now=True)
    def __str__(self):
        return self.ingredient_name
    def added_recently(self):
        return self.ingredient_added_date >= timezone.now() - datetime.timedelta(days=1)
    def update_recently(self):
        return self.ingredient_updated_date >= timezone.now() - datetime.timedelta(days=1)
    
class LevelOneDrugs(models.Model):
    drug_name = models.CharField(max_length=255)
    drug_ingredient = models.ForeignKey(Ingredients, on_delete=models.CASCADE)
    drug_base = models.ForeignKey(BaseDrugs, on_delete=models.CASCADE)
    drug_effects = models.TextField()
    drug_value = models.IntegerField()
    drug_addiction_level = models.DecimalField(max_digits=5, decimal_places=2)
    drug_added_date = models.DateTimeField(auto_now_add=True)
    drug_updated_date = models.DateTimeField(auto_now=True)
    def __str__(self):
        return self.drug_name
    def added_recently(self):
        return self.drug_added_date >= timezone.now() - datetime.timedelta(days=1)
    def update_recently(self):
        return self.drug_updated_date >= timezone.now() - datetime.timedelta(days=1)
