from rest_framework.pagination import PageNumberPagination
from rest_framework.views import Response


class ExamSearchResultTablePagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    page_query_param = 'page'
    max_page_size = 100

    def get_paginated_response(self, data):

        current_query = self.request.META.get('QUERY_STRING', '')

        if current_query and (not current_query.startswith('?')):
            current_query = '?' + current_query

        return Response({
            'pagination': {
                'page': self.page.number,
                'page_size': self.get_page_size(self.request),
                'last_page': self.page.paginator.num_pages,
                'count': self.page.paginator.count,
                'has_next_page': self.page.has_next(),
                'has_prev_page': self.page.has_previous(),
            },
            'results': data,
            'current_query': current_query
        })
