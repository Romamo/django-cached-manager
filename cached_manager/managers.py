# -*- coding:utf-8 -*-
from django.db import models
from django.core.cache import cache


class NotInt(Exception):
    """
    The exception raised when manager is told to convert some string to int,
    but it's impossible.
    """
    pass

class CachedManager(models.Manager):
    def key(self, cache_key, kwargs={}, int_only=True):
        if int_only and kwargs:
            new_kwargs = {}
            for k, v in kwargs.iteritems():
                try:
                    new_kwargs[k] = int(v)
                except (ValueError, TypeError):
                    raise NotInt
            kwargs = new_kwargs
        return self.keys[cache_key] % kwargs

    def _from_cache(self, cache_key, kwargs=None, const_kwargs=None,
                    exclude_kwargs=None,
                    one_item=False, only=None,
                    none_on_error=True, int_only=False, empty_value=-1,
                    values_list=None, flat=False,
                    order_by=None, limit=None):
        """
        Tries to find an object in cache by ``key``. If it's possible
        (``cache.get(key) != empty_value``) the object is returned.
        Else the object is queried, saved in cache and returned.

        Arguments:

        * ``cache_key`` - cache key of the object. If ``kwargs`` argument
        exists, ``cache_key`` should be valid format string to ``kwargs`` dict.
        * ``kwargs`` - dict, holding kwargs to ``filter()`` (or ``.get()``)
        method. ``cache_key`` should be valid format string for this dict. That
        means there will be unique cache key for every distinct ``kwargs``.
        * ``only`` - if is not ``None``, a list of args to ``.only()`` queryset
        method.
        * ``none_on_error`` - if ``True``, ``None`` will be returned on
        ``ObjectDoesNotExist`` and int conversion error. Else exceptions
        will be raised.
        * ``empty_value`` - if ``cache.get(key) != empty_value``, an object
        is in cache.
        * ``one_item`` - if ``True`` the method will try to make ``.get()``
        call and return one object. Else it will make ``.filter()`` call and
        return list.
        * ``const_kwargs`` - same as ``kwargs``, but doesn't affect cache
        key.
        * ``values_list`` - dict, holding values to values_list method
        * ``flat`` - flat to values_list method
        key.
        """
        if kwargs is None:
            kwargs = {}
        try:
            key = self.key(cache_key, kwargs, int_only)
            if const_kwargs is None:
                const_kwargs = {}
            kwargs.update(const_kwargs)
            cached = cache.get(key, empty_value)
            if cached != empty_value:
                return cached
            qset = self.get_query_set()
            if only:
                qset = qset.only(*only)
            if values_list:
                if flat:
                    qset = qset.values_list(*values_list, flat=True)
                else:
                    qset = qset.values_list(*values_list)
            if one_item:
                result = qset.get(**kwargs)
            else:
                qset = qset.filter(**kwargs)
                if exclude_kwargs:
                    qset = qset.exclude(**exclude_kwargs)
                if order_by:
                    qset = qset.order_by(*order_by)
                if limit:
                    qset = qset[:limit]
                result = list(qset)
        except NotInt:
            if none_on_error:
                return None
            else:
                raise NotInt
        except self.model.DoesNotExist:
            if none_on_error:
                result = None
            else:
                raise self.model.DoesNotExist
        cache.set(key, result)
        return result

    def _objects_by_pks(self, get_item_func, pks, cache_key, dict_key='pk',
                        empty_value=-1):
        """
        Returns a list of objects by a list of pk's (or any other fields).

        *``get_item_func`` - a function that returns an object by gived pk. It
        should be something like ``lambda pk: self._from_cache(`cache_key``, {dict_key: pk}, one_item=True, empty_value=empty_value)``
        *``pks`` - a list (or any other iterable) of pks.

        First the method tries to load the objects directly form cache and if
        some objects aren't found, calls ``get_item_func`` for them.
        """
        cached = cache.get_many([cache_key % {dict_key: x} for x in pks])
        results = []
        for pk in pks:
            result = cached.get(cache_key % {dict_key: pk}, empty_value)
            if result == empty_value:
                result = get_item_func(pk)
            results.append(result)
        return results

    def transform(self, tuples, cache_key=None, dict_key='pk',
                        empty_value=-1):
        """
        data - список pk, tuple или list, в которых pk или нулевой элемент должен быть заменен на объект
        Заменяет ID модели на сам объект
        tuples - список tuples или значений.
        Если tuple, то ID - нулевой
        """
        if not tuples or not len(tuples):
            return tuples

        # Подготовка ключей для кеша
        is_list = type(tuples[0]) in [tuple, list]
        is_tuple = is_list and type(tuples[0]) is tuple
        # Словарь id -> ключ
        keys = {}
        for t in tuples:
            if is_list:
                keys[t[0]] = self.key(cache_key, kwargs={dict_key: t[0]})
            else:
                keys[t] = self.key(cache_key, kwargs={dict_key: t})
        # Загрузка всех ключей
        values = cache.get_many(keys.values())

        # Подготовка ключей для загрузки из БД
        objects = {}
        if len(values) != len(keys):
            # список ключей
            not_loaded = set(keys.values()).difference(set(values.keys()))
            if not_loaded:
                # Перевернуть ключи, чтобы по ключу получить id
                keys_un = dict((value, key) for key, value in keys.items())
                not_loaded = [keys_un[x] for x in not_loaded]
                objects = {o.id: o for o in self.get_query_set().filter(**{dict_key+'__in': not_loaded})}

        # Возврат списка объектов
        result = []
        cache_set = {}
        for t in tuples:
            if is_list:
                id = t[0]
            else:
                id = t
            ck = keys[id]
            value = values.get(ck, empty_value)
            if value is empty_value:
                value = objects.get(id, empty_value)
                if value is empty_value:
                    continue
                cache_set[ck] = value
            if is_list:
                if is_tuple:
                    result.append((value,) + t[1:])
                else:
                    result.append([value] + t[1:])
            else:
                result.append(value)
        if cache_set:
            cache.set_many(cache_set)
        return result
