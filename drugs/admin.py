from django.contrib import admin

from .models import BaseDrugs, Ingredients, LevelOneDrugs

admin.site.register(BaseDrugs)
admin.site.register(Ingredients)
admin.site.register(LevelOneDrugs)